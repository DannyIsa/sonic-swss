#include "errormonitororch.h"
#include "portsorch.h"
#include "sai_serialize.h"

ErrorMonitorOrch::ErrorMonitorOrch(
    DBConnector *configDB,
    const std::string &tableName)
    : Orch(configDB, tableName),
      m_configsDB(new DBConnector("CONFIG_DB", 0)),
      m_stateDB(new DBConnector("STATE_DB", 0)),
      m_countersDB(new DBConnector("COUNTERS_DB", 0)),
      m_txErrConfigsTable(new Table(m_configsDB.get(), CFG_PORT_TX_ERROR_TABLE_NAME)),
      m_txErrStatusTable(new Table(m_stateDB.get(), STATE_PORT_TX_ERROR_NAME)),
      m_countersTable(new Table(m_countersDB.get(), COUNTERS_TABLE))
{
    SWSS_LOG_ENTER();

    try {
        getConfigValues();
        validatePollTime();
        initiatePortErrorCounterTable();
    } catch (...) {
        handleError();
    }
    
    startPollingTimer();
}

void ErrorMonitorOrch::doTask(Consumer &consumer)
{
    SWSS_LOG_ENTER();

    try {
        getConfigValuesFromConsumer(consumer);
        validatePollTime();
        flushCycle();
    } catch (...) {
        handleError();
    }

    consumer.m_toSync.clear();
}

void ErrorMonitorOrch::doTask(SelectableTimer &timer)
{
    SWSS_LOG_ENTER();
    
    try {
        flushCycle();
    } catch (...) {
        handleError();
    }
}

void ErrorMonitorOrch::handleError()
{
    SWSS_LOG_ENTER();

    try {
        throw;
    } catch (const DeleteFromConfigurationTableException& e) {
        SWSS_LOG_ERROR("DeleteFromConfigurationTableException: %s", e.what());
        revertToDefaultConfigurations();
    } catch (const InvalidPollTimeException& e) {
        SWSS_LOG_ERROR("InvalidPollTimeException: %s", e.what());
        revertPollingConfig();
    } catch (const InvalidIntException& e) {
        SWSS_LOG_ERROR("InvalidIntException: %s", e.what());
        revertToDefaultConfigurations();
    } catch (const std::exception& e) {
        SWSS_LOG_ERROR("std::exception: %s", e.what());
    }
}

void ErrorMonitorOrch::flushCycle()
{
    SWSS_LOG_ENTER();

    std::map<std::string, swss::Port> &portMap = gPortsOrch->getAllPorts();
    std::vector<PortStats> portsStatuses = computePortsStats(portMap);
    updatePortStatusesTable(portsStatuses);
    logThresholdExceededPorts(portsStatuses);
}

void ErrorMonitorOrch::getConfigValues()
{
    SWSS_LOG_ENTER();
    std::string thresholdStr, pollingIntervalStr;

    if (m_txErrConfigsTable->hget("GLOBAL", THRESHOLD_KEY, thresholdStr))
        m_threshold = strToUintOrError(thresholdStr);
    else
        m_txErrConfigsTable->hset("GLOBAL", THRESHOLD_KEY, std::to_string(DEFAULT_ERR_THRESHOLD));

    if (m_txErrConfigsTable->hget("GLOBAL", POLLING_INTERVAL_KEY, pollingIntervalStr))
        m_pollingInterval = strToUintOrError(pollingIntervalStr);
    else
        m_txErrConfigsTable->hset("GLOBAL", POLLING_INTERVAL_KEY, std::to_string(DEFAULT_POLL_INTERVAL));
}

void ErrorMonitorOrch::validatePollTime()
{
    if (m_pollingInterval < 1) throw InvalidPollTimeException("Polling time must be bigger than 1");
}

void ErrorMonitorOrch::initiatePortErrorCounterTable()
{
    std::map<std::string, swss::Port> &portMap = gPortsOrch->getAllPorts();
    std::vector<PortStats> portsStatuses = initialPortsStats(portMap);
    updatePortStatusesTable(portsStatuses);
}

void ErrorMonitorOrch::startPollingTimer()
{
    SWSS_LOG_ENTER();

    auto interval = timespec { .tv_sec = static_cast<time_t>(m_pollingInterval), .tv_nsec = 0 };
    m_pollingTimer = new SelectableTimer(interval);
    auto executor = new ExecutableTimer(m_pollingTimer, this, "TX_ERR_POLL");
    Orch::addExecutor(executor);
    
    m_pollingTimer->start();
}

void ErrorMonitorOrch::getConfigValuesFromConsumer(Consumer &consumer)
{
    const uint64_t previousPollingInterval = m_pollingInterval;

    auto it = consumer.m_toSync.begin();
    while (it != consumer.m_toSync.end())
    {
        auto &keyOpFieldsValues = it->second;
        auto operation = kfvOp(keyOpFieldsValues);

        if (operation == SET_COMMAND){
            for (auto &fieldValuePair : kfvFieldsValues(keyOpFieldsValues))
            {
                auto &fieldName = fvField(fieldValuePair);
                auto &fieldValue = fvValue(fieldValuePair);

                if (fieldName == THRESHOLD_KEY) m_threshold = strToUintOrError(fieldValue);
                else if (fieldName == POLLING_INTERVAL_KEY) m_pollingInterval = strToUintOrError(fieldValue);
                else SWSS_LOG_WARN("Ignoring unknown error monitor config field '%s' (value '%s')",
                                   fieldName.c_str(), fieldValue.c_str());
            }
        } else if (operation == DEL_COMMAND) {
            throw DeleteFromConfigurationTableException("Invalid case resetting to default config");
        }

        it++;
    }

    if (m_pollingInterval != previousPollingInterval)
    {
        resetPollingTimer();
    }
}

void ErrorMonitorOrch::resetPollingTimer()
{
    SWSS_LOG_ENTER();

    auto newInterval = timespec { .tv_sec = static_cast<time_t>(m_pollingInterval), .tv_nsec = 0 };
    m_pollingTimer->setInterval(newInterval);
    m_pollingTimer->reset();
}

void ErrorMonitorOrch::revertToDefaultConfigurations()
{
    revertThresholdConfig();
    revertPollingConfig();
}

void ErrorMonitorOrch::revertThresholdConfig()
{
    m_threshold = DEFAULT_ERR_THRESHOLD;
    m_txErrConfigsTable->hset("GLOBAL", THRESHOLD_KEY, std::to_string(DEFAULT_ERR_THRESHOLD));
}

void ErrorMonitorOrch::revertPollingConfig()
{
    const uint64_t previousPollingInterval = m_pollingInterval;
    m_pollingInterval = DEFAULT_POLL_INTERVAL;
    m_txErrConfigsTable->hset("GLOBAL", POLLING_INTERVAL_KEY, std::to_string(DEFAULT_POLL_INTERVAL));
    if (m_pollingInterval != previousPollingInterval)
    {
        resetPollingTimer();
    }
}

std::vector<PortStats> ErrorMonitorOrch::initialPortsStats(const std::map<std::string, swss::Port> &portMap)
{
    std::vector<PortStats> portsStats{};

    for (const std::pair<const std::string, swss::Port>& entry : portMap)
    {
        if (entry.second.m_type != Port::PHY) continue;

        uint64_t currentErrorAmount = getPortTxErrorsAmount(sai_serialize_object_id(entry.second.m_port_id));

        PortStats stats{};
        stats.iface = entry.first;
        stats.numOfErrors = 0;
        stats.exceededThreshold = false;
        stats.totalNumOfErrors = currentErrorAmount;

        portsStats.push_back(stats);
    }

    return portsStats;
}

std::vector<PortStats> ErrorMonitorOrch::computePortsStats(const std::map<std::string, swss::Port> &portMap)
{
    std::vector<PortStats> portsStats{};

    for (const std::pair<const std::string, swss::Port>& entry : portMap)
    {
        if (entry.second.m_type != Port::PHY) continue;

        uint64_t prevErrorAmount = getPrevPortTxErrorAmount(entry.first);
        uint64_t currentErrorAmount = getPortTxErrorsAmount(sai_serialize_object_id(entry.second.m_port_id));

        PortStats stats{};
        stats.iface = entry.first;
        stats.numOfErrors = calculateNumOfErrors(prevErrorAmount, currentErrorAmount);
        stats.exceededThreshold = didExceedThreshold(prevErrorAmount, currentErrorAmount);
        stats.totalNumOfErrors = currentErrorAmount;

        portsStats.push_back(stats);
    }

    return portsStats;
}

uint64_t ErrorMonitorOrch::getPortTxErrorsAmount(const std::string &portId)
{
    std::string txErrorsStr {};
    uint64_t txErrors {0};

    if (m_countersTable->hget(
        portId,
        COUNTERS_PORT_TX_ERR_KEY,
        txErrorsStr))
    {
        txErrors = strToUintOrError(txErrorsStr);
    }

    return txErrors;
}

uint64_t ErrorMonitorOrch::getPrevPortTxErrorAmount(const std::string &iface)
{
    std::string portErrorAmountStr {};
    uint64_t portErrorAmount {0};
    
    if (m_txErrStatusTable->hget(
        iface,
        TOTAL_ERR_COUNT_KEY,
        portErrorAmountStr))
    {
        portErrorAmount = strToUintOrError(portErrorAmountStr);
    }

    return portErrorAmount;
}

void ErrorMonitorOrch::updatePortStatusesTable(const std::vector<PortStats>& portsStatuses)
{
    for (const PortStats& ps : portsStatuses)
    {
        m_txErrStatusTable->hset(ps.iface, STATUS_KEY, ps.exceededThreshold ? "Not OK" : "OK");
        m_txErrStatusTable->hset(ps.iface, ERR_COUNT_KEY, std::to_string(ps.numOfErrors));
        m_txErrStatusTable->hset(ps.iface, TOTAL_ERR_COUNT_KEY, std::to_string(ps.totalNumOfErrors));
    }
}

bool ErrorMonitorOrch::didExceedThreshold(uint64_t prevErrorAmount, uint64_t currentErrorAmount)
{
    if (currentErrorAmount < prevErrorAmount)
        return currentErrorAmount >= m_threshold;
    
    return currentErrorAmount - prevErrorAmount >= m_threshold;
}

uint64_t ErrorMonitorOrch::calculateNumOfErrors(uint64_t prevErrorAmount, uint64_t currentErrorAmount)
{
    if (currentErrorAmount < prevErrorAmount) return currentErrorAmount;
    return currentErrorAmount - prevErrorAmount;
}

void ErrorMonitorOrch::logThresholdExceededPorts(const std::vector<PortStats>& portsStatuses)
{
    for (const PortStats& portStat : portsStatuses)
    {
        if (portStat.exceededThreshold)
            SWSS_LOG_CRIT("Port %s exceeded TX error threshold: %" PRIu64 " errors in last cycle",
                portStat.iface.c_str(),
                portStat.numOfErrors);
    }
}

uint64_t ErrorMonitorOrch::strToUintOrError(const std::string& s)
{
    uint64_t sInInt {};
    
    try {
        sInInt = std::stoull(s);
    } catch (const std::invalid_argument&) {
        throw InvalidIntException("Invalid integer format: '" + s + "'");
    }

    return sInInt;
}
