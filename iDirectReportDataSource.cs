using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using NHibernate;
using ClientInfoWcfService.Entities.iDirectReports;
using MySql.Data.MySqlClient;
using NHibernate.Transform;
using System.Configuration;
using ClientInfoWcfService.Common;

namespace ClientInfoWcfService.DAL.DataSources.IDirect
{
    public class iDirectReportDataSource
    {
        private DateTime _startPeriod;
        private DateTime _endPeriod;
        private string _startPeriodString;
        private string _endPeriodString;

        private const string _mysqlDateTimeFormat = "yyyy-MM-dd HH:mm:ss";

        private const string hourTableName = "ota_hour_stats_{0}";
        private const string minuteTableName = "ota_minute_stats_{0}";
        private const string rawTableName = "raw_ota_stats_{0}";

        private const string castHourTableName = "otacast_hour_stats_{0}";
        private const string castMinuteTableName = "otacast_minute_stats_{0}";
        private const string castRawTableName = "raw_otacast_stats_{0}";

        private const int _maxResultFieldsCount = 14;
        private const string _connectionStringName = "iDirectNmsConnectionString";
        private const int _tablesCount = 6;
        private const double _percentileLevel = 0.05;
        private const int _bitsInByte = 8;

        private const int iDirectDbTrueValue = 1;
        private const int iDirectDbFalseValue = 0;

        private enum TrafficType
        {
            Downstream,
            Upstream
        }

        private MySqlConnection _connection = new MySqlConnection(ConfigurationManager.ConnectionStrings[_connectionStringName].ConnectionString);
        private Dictionary<uint, ReportEntry> _reportResults = new Dictionary<uint, ReportEntry>();

        private static class Queries
        {
            /// <summary>
            /// Выбрать все терминалы
            /// </summary>
            public const string GetNetModemsQuery =
                @"SELECT NetModemId, ModemSn, NetModemName, InrouteGroupId, ActiveStatus
                  FROM nms.NetModem
                  WHERE NetModemTypeId = 3 and ModemSn > 1000
                  ORDER BY NetModemName";

            /// <summary>
            /// Выбрать все обратные каналы
            /// </summary>
            public const string GetInrouteGroupsQuery =
                @"SELECT InrouteGroupId, InrouteGroupName, NetworkId
                  FROM nms.InrouteGroup
                  ORDER BY InrouteGroupName";

            /// <summary>
            /// Выбрать все сети
            /// </summary>
            public const string GetNetworksQuery =
                @"SELECT NetworkId, NetworkName
                  FROM nms.Network
                  ORDER BY NetworkName";

            /// <summary>
            /// Выборка почасового трафика из таблиц raw_ota_stats
            /// Параметры - индекс таблицы, начало периода, конец периода, id терминалов
            /// </summary>
            public const string TrafficRawQueryTemplate =
                @"SELECT unique_id,
                        sum(rx_reliable_kbyte) + sum(rx_unreliable_kbyte) + sum(rx_oob_kbyte) UpTotal,
                        sum(tx_reliable_kbyte) + sum(tx_unreliable_kbyte) + sum(tx_oob_kbyte) DownTotal,
                        max(rx_reliable_kbyte + rx_unreliable_kbyte + rx_oob_kbyte)/60 UpMax,
                        max(tx_reliable_kbyte + tx_unreliable_kbyte + tx_oob_kbyte)/60 DownMax
                  FROM nrd_archive.raw_ota_stats_{0}
                  WHERE timestamp between '{1}' AND '{2}' 
                        AND unique_id IN ({3})
                  GROUP BY unique_id";

            /// <summary>
            /// Выборка почасового трафика из таблиц ota_minute_stats
            /// Параметры - индекс таблицы, начало периода, конец периода
            /// </summary>
            public const string TrafficMinuteQueryTemplate =
                @"SELECT unique_id,
                        sum(rx_reliable_kbyte) + sum(rx_unreliable_kbyte) + sum(rx_oob_kbyte) UpTotal,
                        sum(tx_reliable_kbyte) + sum(tx_unreliable_kbyte) + sum(tx_oob_kbyte) DownTotal,
                        max(rx_reliable_max + rx_unreliable_max + rx_oob_max)/60 UpMax,
                        max(tx_reliable_max + tx_unreliable_max + tx_oob_max)/60 DownMax
                  FROM nrd_archive.ota_minute_stats_{0}
                  WHERE timestamp between '{1}' AND '{2}'
                        AND unique_id IN ({3})
                  GROUP BY unique_id";

            /// <summary>
            /// Выборка почасового трафика из таблиц ota_hour_stats
            /// Параметры - индекс таблицы, начало периода, конец периода
            /// </summary>
            public const string TrafficHourQueryTemplate =
                @"SELECT unique_id,
                        rx_reliable_kbyte + rx_unreliable_kbyte + rx_oob_kbyte UpTotal,
                        tx_reliable_kbyte + tx_unreliable_kbyte + tx_oob_kbyte DownTotal,
                        (rx_reliable_max + rx_unreliable_max + rx_oob_max)/60 UpMax,
                        (tx_reliable_max + tx_unreliable_max + tx_oob_max)/60 DownMax
                  FROM nrd_archive.ota_hour_stats_{0}
                  WHERE timestamp between '{1}' AND '{2}'
                        AND unique_id IN ({3})";

            public const string MulticastTrafficQueryTemplate =
                @"SELECT network_id, sum(tx_mcast_kbyte) DownMulticast
                  FROM nrd_archive.{0}
                  WHERE timestamp between '{1}' AND '{2}'
                  GROUP BY network_id";


            /// <summary>
            /// Выборка количества записей для каждого терминала в каждой таблице (ota_minute_stats_0-5, raw_ota_stats_0-5)
            /// Параметры - имя таблицы, id терминала, начало периода, конец периода
            /// </summary>
            public const string QuantitiesQueryTemplate =
                @"SELECT unique_id, count(*)
                  FROM nrd_archive.{0}
                  WHERE timestamp BETWEEN '{1}' AND '{2}'
                        AND unique_id IN ({3})
                  GROUP BY unique_id";

            /// <summary>
            /// Выборка 5% самых больших значений Upstream трафика для терминала
            /// Параметры - имя таблицы, id терминала, начало периода, конец периода, количество записей для терминала
            /// скобочки в запросе важны! чтобы во время UNION LIMIT срабатывал только 
            /// для этой части запроса, а не для всего объединения
            /// </summary>
            public const string TopFivePercentUpstreamTrafficQueryTemplate =
                @"(SELECT SQL_NO_CACHE (rx_reliable_kbyte + rx_unreliable_kbyte + rx_oob_kbyte)/60 trafic
                   FROM nrd_archive.{0}
                   WHERE unique_id = {1} 
                         AND timestamp between '{2}' AND '{3}'
                   ORDER BY trafic DESC
                   LIMIT {4})";

            /// <summary>
            /// Выборка 5% самых больших значений Downstream трафика для терминала
            /// Параметры - имя таблицы, id терминала, начало периода, конец периода, количество записей для терминала
            /// скобочки в запросе важны! чтобы во время UNION LIMIT срабатывал только 
            /// для этой части запроса, а не для всего объединения
            /// </summary>
            public const string TopFivePercentDownstreamTrafficQueryTemplate =
                @"(SELECT SQL_NO_CACHE (a.tx_reliable_kbyte + a.tx_unreliable_kbyte + a.tx_oob_kbyte + b.tx_mcast_kbyte)/60 trafic
                   FROM nrd_archive.{0} a
                   LEFT JOIN nrd_archive.{1} b ON a.timestamp = b.timestamp AND a.network_id = b.network_id
                   WHERE unique_id = {2}
                         AND a.timestamp between '{3}' AND '{4}'
                   ORDER BY trafic DESC
                   LIMIT {5})";

            /// <summary>
            /// Выборка данных терминалов (имя, серийный номер, DID, MIR, id несущих)
            /// Параметры - список id терминалов через запятую
            /// </summary>
            public const string NetModemMirQueryTemplate =
                @"SELECT NetModemId, ModemSn, NetModemName, DID, ActiveStatus,
                        UseInrouteMaximumDataRate, InrouteMaximumDataRate,
                        UseOutrouteMaximumDataRate, OutrouteMaximumDataRate, 
                        InrouteGroupId, NetworkId
                  FROM nms.NetModem
                  WHERE NetModemId IN ({0})";

            /// <summary>
            /// Выборка CIR для всех терминалов
            /// </summary>
            public const string NetModemCirQuery =
                @"SELECT RemoteId, Direction, sum(CIR_BPS)
                  FROM nms.VirtualRemote
                  GROUP BY RemoteId, Direction";

            /// <summary>
            /// Параметры - InrouteGroupId для терминала
            /// </summary>
            public const string GetDownMirQuery =
                @"SELECT BitRate 
                  FROM nms.NetModem 
                  JOIN nms.Carrier ON Carrier.CarrierId = NetModem.TxCarrierId
                  WHERE NetModem.InrouteGroupId = {0} AND NetModem.NetModemTypeId <> 3";
        }


        public DateTime StartPeriod
        {
            get
            {
                return _startPeriod;
            }
        }

        public DateTime EndPeriod
        {
            get
            {
                return _endPeriod;
            }
        }

        /// <summary>
        /// Получить данные отчёта
        /// </summary>
        /// <param name="selectedTerminals">список id терминалов, по которым нужен отчет</param>
        /// <returns>словарь id терминала - данные отчета</returns>
        public List<ReportEntry> GetReport(DateTime startPeriod, DateTime endPeriod, List<uint> selectedTerminals)
        {
            Connect();
            _startPeriod = startPeriod;
            _endPeriod = endPeriod;
            _startPeriodString = startPeriod.ToString(_mysqlDateTimeFormat);
            _endPeriodString = endPeriod.ToString(_mysqlDateTimeFormat);

            List<uint> dataExistsForTerminals = new List<uint>();

            if (selectedTerminals.Count == 0)
                return _reportResults.Values.ToList();

            string terminalIdString = GetTerminalIdString(selectedTerminals);

            DateTime startReportGeneration = DateTime.Now;
            ServicesHelper.WriteDebugToDisk(string.Format(DateTime.Now.ToString() +
                "Запрос статистики для терминалов iDirect с id {0} за период с {1} по {2}\n",
                terminalIdString, _startPeriodString, _endPeriodString));

            Dictionary<uint, long> quantities = GetTerminalListAndQuantities(terminalIdString);

            GetPercentiles(quantities);

            GetTotalTrafficAndMaxSpeed(terminalIdString);

            GetMirAndTerminalInfo(terminalIdString);

            GetMulticastTraffic();

            GetCir();

            GetMediumSpeedAndBandwidthUtilization();

            Disconnect();

            ServicesHelper.WriteDebugToDisk(string.Format(DateTime.Now.ToString() +
                "Запрос статистики для терминалов iDirect выполнился за {0}\n", (DateTime.Now - startReportGeneration).TotalSeconds));

            return _reportResults.Values.ToList();
        }

        public List<Network> GetTerminalsTree()
        {
            Connect();
            List<Network> networks = GetNetworks();
            List<InrouteGroup> inrouteGroups = GetInrouteGroups();
            List<NetModem> netModems = GetNetModems();
            Disconnect();

            //добавляем узлы сетей
            foreach (Network network in networks)
            {
                network.InrouteGroups = new List<InrouteGroup>();
            }

            //добавляем узлы обратных каналов 
            foreach (InrouteGroup group in inrouteGroups)
            {
                foreach (Network network in networks)
                {
                    if (network.InrouteGroups == null)
                        network.InrouteGroups = new List<InrouteGroup>();
                    if (network.Id == group.NetworkId)
                    {
                        network.InrouteGroups.Add(group);
                    }
                }
            }

            //добавляем узлы терминалов
            foreach (NetModem modem in netModems)
            {
                foreach (Network network in networks)
                {
                    foreach (InrouteGroup group in network.InrouteGroups)
                    {
                        if (group.NetModems == null)
                            group.NetModems = new List<NetModem>();
                        if (modem.InrouteGroupId == group.Id)
                        {
                            group.NetModems.Add(modem);
                        }
                    }
                }
            }

            return networks;
        }


        /// <summary>
        /// Получить список сетей
        /// </summary>
        /// <returns></returns>
        private List<Network> GetNetworks()
        {
            List<Network> networks = new List<Network>();

            MySqlCommand command = new MySqlCommand(Queries.GetNetworksQuery, _connection);
            MySqlDataReader reader = command.ExecuteReader();
            object[] values = new object[_maxResultFieldsCount];

            while (reader.Read())
            {
                reader.GetValues(values);
                Network currentNetwork = new Network();
                currentNetwork.Id = Convert.ToUInt32(values[0]);
                currentNetwork.Name = values[1].ToString();
                networks.Add(currentNetwork);
            }
            reader.Close();

            return networks;
        }

        /// <summary>
        /// Получить список обратных каналов
        /// </summary>
        /// <returns></returns>
        private List<InrouteGroup> GetInrouteGroups()
        {
            List<InrouteGroup> inrouteGroups = new List<InrouteGroup>();

            MySqlCommand command = new MySqlCommand(Queries.GetInrouteGroupsQuery, _connection);
            MySqlDataReader reader = command.ExecuteReader();
            object[] values = new object[_maxResultFieldsCount];

            while (reader.Read())
            {
                reader.GetValues(values);
                InrouteGroup currentInrouteGroup = new InrouteGroup();
                currentInrouteGroup.Id = Convert.ToUInt32(values[0]);
                currentInrouteGroup.Name = values[1].ToString();
                currentInrouteGroup.NetworkId = Convert.ToUInt32(values[2]);
                inrouteGroups.Add(currentInrouteGroup);
            }
            reader.Close();

            return inrouteGroups;
        }

        /// <summary>
        /// Получить полный список терминалов
        /// </summary>
        /// <returns></returns>
        private List<NetModem> GetNetModems()
        {
            List<NetModem> netModems = new List<NetModem>();

            MySqlCommand command = new MySqlCommand(Queries.GetNetModemsQuery, _connection);
            MySqlDataReader reader = command.ExecuteReader();
            object[] values = new object[_maxResultFieldsCount];

            while (reader.Read())
            {
                reader.GetValues(values);
                NetModem currentNetModem = new NetModem();
                currentNetModem.Id = Convert.ToUInt32(values[0]);
                currentNetModem.Name = values[2].ToString() + " S/N " + values[1].ToString();
                currentNetModem.InrouteGroupId = Convert.ToUInt32(values[3]);
                currentNetModem.ActiveStatus = Convert.ToInt16(values[4]);
                if (currentNetModem.ActiveStatus == iDirectDbFalseValue)
                    currentNetModem.Name += " (выключен)";
                netModems.Add(currentNetModem);
            }
            reader.Close();

            return netModems;
        }

        /// <summary>
        /// Соединиться с БД
        /// </summary>
        /// <returns>true, если соединение успешно. иначе - ApplicationException</returns>
        public bool Connect()
        {
            try
            {
                VPNHelper.CheckControlNetwork();
                _connection.Open();
            }
            catch (Exception exc)
            {
                throw new ApplicationException("Невозможно подключиться к БД", exc);
            }
            return true;
        }

        /// <summary>
        /// Отсоединиться от БД
        /// </summary>
        /// <returns>true</returns>
        public bool Disconnect()
        {
            try
            {
                _connection.Close();
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Получить список терминалов, которые попадут в отчёт и количество записей для каждого из них в таблицах
        /// </summary>
        /// <param name="selectedTerminals">список id запрошенных пользователем терминалов</param>
        /// <returns></returns>
        private Dictionary<uint, long> GetTerminalListAndQuantities(string selectedTerminalsIdString)
        {
            Dictionary<uint, long> quantities = new Dictionary<uint, long>();

            string query = GenerateTerminalListAndQuantitiesQuery(selectedTerminalsIdString);

            MySqlCommand command = new MySqlCommand(query, _connection);
            MySqlDataReader reader = command.ExecuteReader();
            object[] values = new object[_maxResultFieldsCount];

            while (reader.Read())
            {
                reader.GetValues(values);

                uint uniqueId = Convert.ToUInt32(values[0]);
                long quantity = Convert.ToInt64(values[1]);

                if (!_reportResults.ContainsKey(uniqueId))
                {
                    _reportResults.Add(uniqueId, new ReportEntry(uniqueId));
                }

                if (!quantities.ContainsKey(uniqueId))
                {
                    quantities.Add(uniqueId, quantity);
                }
                else
                {
                    quantities[uniqueId] += quantity;
                }
            }
            reader.Close();

            return quantities;
        }

        /// <summary>
        /// Получить процентили скорости
        /// </summary>
        /// <param name="quantities"></param>
        private void GetPercentiles(Dictionary<uint, long> quantities)
        {
            //пока за периоды ранее 31 дня перцентили не считаем
            if ((DateTime.Now - _startPeriod).Days >= 31)
                return;
            /*
 5.1. Выбрать 0.05*[количество записей для терминала] самых больших значений 
    входящего трафика из каждой таблицы ota_minute_stats.
 5.2. Полученные в п.5.1. данные отсортировать по убыванию, отбросить 
    верхние 0.05*[количество записей для терминала] записей, из оставшегося 
    списка взять первую запись. В ней содержится минутный перцентиль входящего 
    трафика для данного терминала. Запомнить его в общих результатах.
 5.3. Повторить пп.5.1.-5.2. для исходящего трафика.
  */

            long maxQuantity = 1;
            foreach (KeyValuePair<uint, long> quantityEntry in quantities)
            {
                if (quantityEntry.Value > maxQuantity)
                    maxQuantity = quantityEntry.Value;
            }
            int topFivePercentQuantity = (int)Math.Round(_percentileLevel * maxQuantity / 6 + 15); //это особая магия

            foreach (KeyValuePair<uint, long> quantityEntry in quantities)
            {
                if (_reportResults.ContainsKey(quantityEntry.Key))
                {
                    string upstreamQuery = GenerateTopFivePercentQuery(quantityEntry.Key, TrafficType.Upstream, topFivePercentQuantity);
                    _reportResults[quantityEntry.Key].UpPercentile = GetPercentile(upstreamQuery, quantityEntry.Value);

                    string downstreamQuery = GenerateTopFivePercentQuery(quantityEntry.Key, TrafficType.Downstream, topFivePercentQuantity);
                    _reportResults[quantityEntry.Key].DownPercentile = GetPercentile(downstreamQuery, quantityEntry.Value);
                }
            }
        }

        /// <summary>
        /// Получить процентиль
        /// </summary>
        /// <param name="query">запрос для получения самых больших записей из каждой таблицы</param>
        /// <param name="quantity">количество отбрасываемых записей</param>
        /// <returns></returns>
        private double GetPercentile(string query, long quantity)
        {
            List<double> trafficQuantities = new List<double>();
            MySqlCommand command = new MySqlCommand(query, _connection);
            MySqlDataReader reader = command.ExecuteReader();
            object[] values = new object[_maxResultFieldsCount];
            while (reader.Read())
            {
                reader.GetValues(values);
                double trafficQuantity = Convert.ToDouble(values[0]);
                trafficQuantities.Add(trafficQuantity);
            }
            reader.Close();
            List<double> sortedQuantities = trafficQuantities.OrderByDescending(e => e).ToList();
            int percentileIndex = (int)Math.Round(_percentileLevel * quantity);
            return sortedQuantities[percentileIndex] * _bitsInByte;
        }

        /// <summary>
        /// Сгенерировать запрос для получения самых больших значений скорости из каждой таблицы для определенного терминала
        /// </summary>
        /// <param name="terminalId">id терминала</param>
        /// <param name="trafficType">направление трафика</param>
        /// <param name="quantity">количество записей, получаемых из каждой таблицы</param>
        /// <returns></returns>
        private string GenerateTopFivePercentQuery(uint terminalId, TrafficType trafficType, long quantity)
        {
            string resultQuery = string.Empty;
            string queryTemplate = string.Empty;
            if (trafficType == TrafficType.Downstream)
                queryTemplate = Queries.TopFivePercentDownstreamTrafficQueryTemplate;
            else if (trafficType == TrafficType.Upstream)
                queryTemplate = Queries.TopFivePercentUpstreamTrafficQueryTemplate;

            for (int tableIndex = 0; tableIndex < _tablesCount; tableIndex++)
            {
                string query = string.Empty;

                //ota_minute_stats
                string tableName = string.Format(minuteTableName, tableIndex);
                string castTableName = string.Format(castMinuteTableName, tableIndex);
                if (trafficType == TrafficType.Upstream)
                {
                    query = string.Format(queryTemplate, tableName, terminalId, _startPeriodString, _endPeriodString, quantity);
                }
                else if (trafficType == TrafficType.Downstream)
                {
                    query = string.Format(queryTemplate, tableName, castTableName, terminalId, _startPeriodString, _endPeriodString, quantity);
                }
                resultQuery = UnionQueries(resultQuery, query);

                //raw_ota_stats
                tableName = string.Format(rawTableName, tableIndex);
                castTableName = string.Format(castRawTableName, tableIndex);
                if (trafficType == TrafficType.Upstream)
                {
                    query = string.Format(queryTemplate, tableName, terminalId, _startPeriodString, _endPeriodString, quantity);
                }
                else if (trafficType == TrafficType.Downstream)
                {
                    query = string.Format(queryTemplate, tableName, castTableName, terminalId, _startPeriodString, _endPeriodString, quantity);
                }
                resultQuery = UnionQueries(resultQuery, query);

                //ota_hour_stats
                tableName = string.Format(hourTableName, tableIndex);
                castTableName = string.Format(castHourTableName, tableIndex);
                if (trafficType == TrafficType.Upstream)
                {
                    query = string.Format(queryTemplate, tableName, terminalId, _startPeriodString, _endPeriodString, quantity);
                }
                else if (trafficType == TrafficType.Downstream)
                {
                    query = string.Format(queryTemplate, tableName, castTableName, terminalId, _startPeriodString, _endPeriodString, quantity);
                }
                resultQuery = UnionQueries(resultQuery, query);
            }

            return resultQuery;
        }

        /// <summary>
        /// Получить общее количество трафика и максимальную скорость
        /// </summary>
        /// <param name="selectedTerminalsIdString">список выбранных пользователем терминалов</param>
        private void GetTotalTrafficAndMaxSpeed(string selectedTerminalsIdString)
        {
            string trafficQueries = GetTrafficQueries(selectedTerminalsIdString);

            MySqlCommand command = new MySqlCommand(trafficQueries, _connection);
            MySqlDataReader reader = command.ExecuteReader();
            object[] values = new object[_maxResultFieldsCount];
            while (reader.Read())
            {
                reader.GetValues(values);
                uint uniqueId = Convert.ToUInt32(values[0]);
                double upTotal = Convert.ToDouble(values[1]);
                double downTotal = Convert.ToDouble(values[2]);
                double upMax = Convert.ToDouble(values[3]);
                double downMax = Convert.ToDouble(values[4]);

                if (_reportResults.ContainsKey(uniqueId))
                {
                    _reportResults[uniqueId].UpTotal += upTotal;
                    _reportResults[uniqueId].DownTotal += downTotal;
                    if (upMax > _reportResults[uniqueId].UpMaxSpeed)
                    {
                        _reportResults[uniqueId].UpMaxSpeed = upMax * _bitsInByte;
                    }
                    if (downMax > _reportResults[uniqueId].DownMaxSpeed)
                    {
                        _reportResults[uniqueId].DownMaxSpeed = downMax * _bitsInByte;
                    }
                }
                else
                {
                }
            }
            reader.Close();
        }

        /// <summary>
        /// Получить MIR и общую информацию для терминалов
        /// </summary>
        /// <param name="terminals">список id терминалов, для которых получать данные</param>
        private void GetMirAndTerminalInfo(string selectedTerminalsIdString)
        {
            string query = string.Format(Queries.NetModemMirQueryTemplate, selectedTerminalsIdString);

            MySqlCommand command = new MySqlCommand(query, _connection);
            MySqlDataReader reader = command.ExecuteReader();
            object[] values = new object[_maxResultFieldsCount];
            while (reader.Read())
            {
                reader.GetValues(values);
                uint uniqueId = Convert.ToUInt32(values[0]);
                uint ModemSn = Convert.ToUInt32(values[1]);
                string NetModemName = Convert.ToString(values[2]);
                uint DID = Convert.ToUInt32(values[3]);
                short ActiveStatus = Convert.ToInt16(values[4]);
                uint UseInrouteMaximumDataRate = Convert.ToUInt32(values[5]);
                double InrouteMaximumDataRate = Convert.ToDouble(values[6]);
                uint UseOutrouteMaximumDataRate = Convert.ToUInt32(values[7]);
                double OutrouteMaximumDataRate = Convert.ToDouble(values[8]);
                uint InrouteGroupId = Convert.ToUInt32(values[9]);
                uint NetworkId = Convert.ToUInt32(values[10]);

                if (_reportResults.ContainsKey(uniqueId))
                {
                    _reportResults[uniqueId].SerialNumber = ModemSn;
                    _reportResults[uniqueId].DID = DID;
                    _reportResults[uniqueId].TerminalName = NetModemName;
                    _reportResults[uniqueId].InrouteGroupId = InrouteGroupId;
                    _reportResults[uniqueId].NetworkId = NetworkId;
                    if (UseInrouteMaximumDataRate == iDirectDbTrueValue)
                    {
                        _reportResults[uniqueId].UpMir = InrouteMaximumDataRate;
                    }
                    if (UseOutrouteMaximumDataRate == iDirectDbTrueValue)
                    {
                        _reportResults[uniqueId].DownMir = OutrouteMaximumDataRate;
                    }
                }
                else
                {
                }
            }
            reader.Close();

            foreach (ReportEntry reportEntry in _reportResults.Values)
            {
                if (reportEntry.DownMir == 0)
                    reportEntry.DownMir = GetDownMir(reportEntry.InrouteGroupId);
            }
        }

        private void GetMulticastTraffic()
        {
            string multicastQueries = GenerateMulticastTrafficQuery();

            Dictionary<uint, double> queryResult = new Dictionary<uint, double>();

            MySqlCommand command = new MySqlCommand(multicastQueries, _connection);
            MySqlDataReader reader = command.ExecuteReader();
            object[] values = new object[_maxResultFieldsCount];
            while (reader.Read())
            {
                reader.GetValues(values);
                uint networkId = Convert.ToUInt32(values[0]);
                double downMulticastTraffic = Convert.ToDouble(values[1]);

                if (!queryResult.ContainsKey(networkId))
                {
                    queryResult.Add(networkId, downMulticastTraffic);
                }
                else
                {
                    queryResult[networkId] += downMulticastTraffic;
                }
            }
            reader.Close();

            foreach (KeyValuePair<uint, ReportEntry> reportEntry in _reportResults)
            {
                reportEntry.Value.DownTotal += queryResult[reportEntry.Value.NetworkId];
            }
        }

        /// <summary>
        /// Получить MIR для обратного канала
        /// </summary>
        /// <param name="inrouteGroupId">id обратного канала</param>
        /// <returns></returns>
        private double GetDownMir(uint inrouteGroupId)
        {
            string query = string.Format(Queries.GetDownMirQuery, inrouteGroupId);
            MySqlCommand command = new MySqlCommand(query, _connection);
            object result = command.ExecuteScalar();
            if (result != null && result is double)
                return Convert.ToDouble(result);
            else
                return 0;
        }

        /// <summary>
        /// Получить CIR для списка терминалов
        /// </summary>
        private void GetCir()
        {
            string query = Queries.NetModemCirQuery;

            MySqlCommand command = new MySqlCommand(query, _connection);
            MySqlDataReader reader = command.ExecuteReader();
            object[] values = new object[_maxResultFieldsCount];
            while (reader.Read())
            {
                reader.GetValues(values);
                uint uniqueId = Convert.ToUInt32(values[0]);
                short direction = Convert.ToInt16(values[1]);
                double cir = Convert.ToDouble(values[2]) / 1000;

                if (_reportResults.ContainsKey(uniqueId))
                {
                    if (direction == 1)
                        _reportResults[uniqueId].UpCir = cir;
                    else if (direction == 0)
                        _reportResults[uniqueId].DownCir = cir;
                }
                else
                {
                }
            }
            reader.Close();
        }

        /// <summary>
        /// Рассчитать средние скорости и утилизацию канала
        /// </summary>
        private void GetMediumSpeedAndBandwidthUtilization()
        {
            int secondsCount = (int)(_endPeriod - _startPeriod).TotalSeconds;
            foreach (KeyValuePair<uint, ReportEntry> reportEntry in _reportResults)
            {
                reportEntry.Value.DownMediumSpeed = reportEntry.Value.DownTotal / secondsCount * _bitsInByte;
                reportEntry.Value.UpMediumSpeed = reportEntry.Value.UpTotal / secondsCount * _bitsInByte;
                //Терешков 20.06.12: В общем, предлагаю заменить столбцы максимальной полосы в прямом и обратном канале на 95%-е перцентили. И загрузку (в % или долях) считать исходя из них, а не из средней скорости.
                /*if (reportEntry.Value.DownMir != 0)
                    reportEntry.Value.DownBandwidthUsagePercent = reportEntry.Value.DownMediumSpeed / reportEntry.Value.DownMir * 100;
                if (reportEntry.Value.UpMir != 0)
                    reportEntry.Value.UpBandwidthUsagePercent = reportEntry.Value.UpMediumSpeed / reportEntry.Value.UpMir * 100;*/
                if (reportEntry.Value.DownMir != 0)
                    reportEntry.Value.DownBandwidthUsagePercent = reportEntry.Value.DownPercentile / reportEntry.Value.DownMir * 100;
                if (reportEntry.Value.UpMir != 0)
                    reportEntry.Value.UpBandwidthUsagePercent = reportEntry.Value.UpPercentile / reportEntry.Value.UpMir * 100;
            }
        }

        /// <summary>
        /// Сгенерировать запрос для получения списка id терминала - количество записей для него в каждой таблице
        /// </summary>
        /// <returns></returns>
        private string GenerateTerminalListAndQuantitiesQuery(string selectedTerminalsIdString)
        {
            string resultQuery = string.Empty;
            for (int tableIndex = 0; tableIndex < _tablesCount; tableIndex++)
            {
                string tableName = string.Format(hourTableName, tableIndex);

                //ota_hour_stats
                string quantitiesQuery = string.Format(Queries.QuantitiesQueryTemplate, tableName, _startPeriodString, _endPeriodString, selectedTerminalsIdString);
                resultQuery = UnionQueries(resultQuery, quantitiesQuery);

                tableName = string.Format(minuteTableName, tableIndex);
                //ota_minute_stats
                quantitiesQuery = string.Format(Queries.QuantitiesQueryTemplate, tableName, _startPeriodString, _endPeriodString, selectedTerminalsIdString);
                resultQuery = UnionQueries(resultQuery, quantitiesQuery);

                //raw_ota_stats
                tableName = string.Format(rawTableName, tableIndex);
                quantitiesQuery = string.Format(Queries.QuantitiesQueryTemplate, tableName, _startPeriodString, _endPeriodString, selectedTerminalsIdString);
                resultQuery = UnionQueries(resultQuery, quantitiesQuery);
            }
            return resultQuery;
        }

        /// <summary>
        /// Сгенерировать запрос для получения списка id терминала - количество записей для него в каждой таблице
        /// </summary>
        /// <returns></returns>
        private string GenerateMulticastTrafficQuery()
        {
            string resultQuery = string.Empty;
            for (int tableIndex = 0; tableIndex < _tablesCount; tableIndex++)
            {
                //ota_hour_stats
                string tableName = string.Format(castHourTableName, tableIndex);
                string multicastQuery = string.Format(Queries.MulticastTrafficQueryTemplate, tableName, _startPeriodString, _endPeriodString);
                resultQuery = UnionQueries(resultQuery, multicastQuery);

                //ota_minute_stats
                tableName = string.Format(castMinuteTableName, tableIndex);
                multicastQuery = string.Format(Queries.MulticastTrafficQueryTemplate, tableName, _startPeriodString, _endPeriodString);
                resultQuery = UnionQueries(resultQuery, multicastQuery);

                //raw_ota_stats
                tableName = string.Format(castRawTableName, tableIndex);
                multicastQuery = string.Format(Queries.MulticastTrafficQueryTemplate, tableName, _startPeriodString, _endPeriodString);
                resultQuery = UnionQueries(resultQuery, multicastQuery);
            }
            return resultQuery;
        }


        /// <summary>
        /// Сгенерировать запрос для получения общего количества трафика и максимальных скоростей
        /// </summary>
        /// <param name="selectedTerminalsIdString">список выбранных пользователем терминалов</param>
        /// <returns></returns>
        private string GetTrafficQueries(string selectedTerminalsIdString)
        {
            const int HoursCountInRawTable = -24;
            const int HoursCountInMinuteTable = -744;
            const int HoursCountInHourTable = -4464;

            DateTime hourStartPeriod = DateTime.Now.AddHours(HoursCountInHourTable);
            DateTime hourEndPeriod = DateTime.Now.AddHours(HoursCountInMinuteTable);
            DateTime minuteStartPeriod = DateTime.Now.AddHours(HoursCountInMinuteTable);
            DateTime minuteEndPeriod = DateTime.Now.AddHours(HoursCountInRawTable);
            DateTime rawStartPeriod = DateTime.Now.AddHours(HoursCountInRawTable);
            DateTime rawEndPeriod = DateTime.Now;

            bool useHourTable = false;
            bool useMinuteTable = false;
            bool useRawTable = false;

            if (_startPeriod < hourStartPeriod)
            {
                useHourTable = true;
            }
            if (_startPeriod >= hourStartPeriod && _startPeriod < hourEndPeriod)
            {
                hourStartPeriod = _startPeriod;
                useHourTable = true;
            }
            else if (_startPeriod >= minuteStartPeriod && _startPeriod < minuteEndPeriod)
            {
                minuteStartPeriod = _startPeriod;
                useMinuteTable = true;
            }
            else if (_startPeriod >= rawStartPeriod && _startPeriod < rawEndPeriod)
            {
                rawStartPeriod = _startPeriod;
                useRawTable = true;
            }
            else if (_startPeriod >= rawEndPeriod)
            {
                //нет данных
            }


            if (_endPeriod < hourStartPeriod)
            {
                //нет данных
            }
            if (_endPeriod >= hourStartPeriod && _endPeriod < hourEndPeriod)
            {
                hourEndPeriod = _endPeriod;
                useHourTable = true;
            }
            else if (_endPeriod >= minuteStartPeriod && _endPeriod < minuteEndPeriod)
            {
                minuteEndPeriod = _endPeriod;
                useMinuteTable = true;
            }
            else if (_endPeriod >= rawStartPeriod && _endPeriod < rawEndPeriod)
            {
                rawEndPeriod = _endPeriod;
                useRawTable = true;
            }

            string trafficQueries = string.Empty;

            if (useHourTable)
                trafficQueries += GenerateTrafficQueriesForTableType(hourTableName, hourStartPeriod, hourEndPeriod, selectedTerminalsIdString);

            if (useMinuteTable)
            {
                string trafQuery = GenerateTrafficQueriesForTableType(minuteTableName, minuteStartPeriod, minuteEndPeriod, selectedTerminalsIdString);
                trafficQueries = UnionQueries(trafficQueries, trafQuery);
            }

            if (useRawTable)
            {
                string trafQuery = GenerateTrafficQueriesForTableType(rawTableName, minuteStartPeriod, minuteEndPeriod, selectedTerminalsIdString);
                trafficQueries = UnionQueries(trafficQueries, trafQuery);
            }

            return trafficQueries;
        }



        /// <summary>
        /// Сгенерировать запрос для получения общего количества трафика и максимальных скоростей для определенного вида таблицы
        /// </summary>
        /// <returns></returns>
        private string GenerateTrafficQueriesForTableType(
            string tableTypeName,
            DateTime tableTypeStartPeriod,
            DateTime tableTypeEndPeriod,
            string selectedTerminalsIdString)
        {
            string trafficQueryTemplate = string.Empty;
            if (tableTypeName == rawTableName)
                trafficQueryTemplate = Queries.TrafficRawQueryTemplate;
            else if (tableTypeName == minuteTableName)
                trafficQueryTemplate = Queries.TrafficMinuteQueryTemplate;
            else if (tableTypeName == hourTableName)
                trafficQueryTemplate = Queries.TrafficHourQueryTemplate;
            else
                throw new ApplicationException("Неверное имя таблицы");

            string startPeriodString = tableTypeStartPeriod.ToString(_mysqlDateTimeFormat);
            string endPeriodString = tableTypeEndPeriod.ToString(_mysqlDateTimeFormat);

            string resultQuery = string.Empty;
            for (int currentTableIndex = 0; currentTableIndex < _tablesCount; currentTableIndex++)
            {
                string trafficQuery = string.Format(trafficQueryTemplate, currentTableIndex, startPeriodString, endPeriodString, selectedTerminalsIdString);
                resultQuery = UnionQueries(resultQuery, trafficQuery);
            }

            return resultQuery;
        }

        /// <summary>
        /// Получить индекс таблицы по дате и времени.
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        private int GetTableIndexByDateTime(DateTime date)
        {
            DateTime startOfEpoch = new DateTime(1970, 1, 1);
            long linuxDate = (long)(date.ToUniversalTime() - startOfEpoch).TotalMilliseconds;
            int tableIndex = (int)((linuxDate - 1072915201) / 21600) % 6;
            return tableIndex;
        }

        private string GetTerminalIdString(List<uint> selectedTerminals)
        {
            string terminalIdString = string.Empty;
            foreach (uint terminalId in selectedTerminals)
            {
                terminalIdString += terminalId.ToString() + ", ";
            }
            terminalIdString = terminalIdString.Remove(terminalIdString.Length - 2);
            return terminalIdString;
        }

        private string UnionQueries(string firstQuery, string secondQuery)
        {
            if (string.IsNullOrEmpty(firstQuery))
            {
                firstQuery += secondQuery;
            }
            else
            {
                firstQuery += " UNION ALL ";
                firstQuery += secondQuery;
            }
            return firstQuery;
        }
    }
}