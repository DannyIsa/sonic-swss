#ifndef ERROR_MONITOR_ORCH_H
#define ERROR_MONITOR_ORCH_H

#include "orch.h"
#include "timer.h"
#include "logger.h"
#include "port.h"
#include <cstdint>
#include <cinttypes>
#include <exception>
#include <stdexcept>
#include <string>
#include <memory>

class PortsOrch;

#define DEFAULT_ERR_THRESHOLD 100
#define DEFAULT_POLL_INTERVAL 30 // seconds

#define THRESHOLD_KEY            "threshold"
#define POLLING_INTERVAL_KEY     "polling_interval"
#define STATUS_KEY               "status"
#define ERR_COUNT_KEY            "tx_err_count"
#define TOTAL_ERR_COUNT_KEY      "total_tx_err_count"
#define COUNTERS_PORT_TX_ERR_KEY "SAI_PORT_STAT_IF_OUT_ERRORS"

extern PortsOrch *gPortsOrch;

struct PortStats
{
    std::string iface;
    bool exceededThreshold;
    uint64_t numOfErrors;
    uint64_t totalNumOfErrors;
};

class ErrorMonitorOrch : public Orch {
public:
    ErrorMonitorOrch(swss::DBConnector *db, const std::string &tableName);

    virtual void doTask(swss::SelectableTimer &timer);
    virtual void doTask(Consumer &consumer);

    class InvalidIntException : public std::invalid_argument
    {
    public:
        explicit InvalidIntException(const std::string& message)
            : std::invalid_argument(message) {}
    };
    class DeleteFromConfigurationTableException : public std::logic_error
    {
    public:
        explicit DeleteFromConfigurationTableException(const std::string& message)
            : std::logic_error(message) {}
    };
    class InvalidPollTimeException : public std::invalid_argument
    {
    public:
        explicit InvalidPollTimeException(const std::string& message)
            : std::invalid_argument(message) {}
    };

private:
    void flushCycle();
    void getConfigValues();
    void validatePollTime();
    void getConfigValuesFromConsumer(Consumer &consumer);
    void startPollingTimer();
    void resetPollingTimer();
    void initiatePortErrorCounterTable();
    std::vector<PortStats> initialPortsStats(const std::map<std::string, swss::Port> &portMap);
    std::vector<PortStats> computePortsStats(const std::map<std::string, swss::Port> &portMap);
    void updatePortStatusesTable(const std::vector<PortStats>& portsStatuses);
    void logThresholdExceededPorts(const std::vector<PortStats>& portsStatuses);
    uint64_t getPortTxErrorsAmount(const std::string &portId);
    uint64_t getPrevPortTxErrorAmount(const std::string &iface);
    bool didExceedThreshold(uint64_t prevErrorAmount, uint64_t currentErrorAmount);
    uint64_t calculateNumOfErrors(uint64_t prevErrorAmount, uint64_t currentErrorAmount);
    uint64_t strToUintOrError(const std::string& s);
    void handleError();
    void revertToDefaultConfigurations();
    void revertThresholdConfig();
    void revertPollingConfig();
    
    uint64_t m_threshold =       DEFAULT_ERR_THRESHOLD;
    uint64_t m_pollingInterval = DEFAULT_POLL_INTERVAL;

    std::shared_ptr<swss::DBConnector> m_configsDB = nullptr;
    std::shared_ptr<swss::DBConnector> m_stateDB = nullptr;
    std::shared_ptr<swss::DBConnector> m_countersDB = nullptr;
    std::shared_ptr<swss::Table> m_txErrConfigsTable = nullptr;
    std::shared_ptr<swss::Table> m_txErrStatusTable = nullptr;
    std::shared_ptr<swss::Table> m_countersTable = nullptr;

    SelectableTimer *m_pollingTimer = nullptr;
};

#endif
