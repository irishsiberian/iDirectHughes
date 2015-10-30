using System;
using System.Collections.Generic;
using ClientInfoWcfService.DAL.Core;
using ClientInfoWcfService.Entities;
using NHibernate;
using NHibernate.Transform;
using System.Linq;
using ClientInfoWcfService.Common;
using System.Text;
using System.Reflection;
using NHibernate.Loader.Custom;
using System.Collections;
using ClientInfoWcfService.Entities.HughesSpeedTest;

namespace ClientInfoWcfService.DAL.DataSources
{
    public class HughesSpeedTestDataSource : PageAbleDataSourceBase<HughesSpeedTest>
    {
        private ISession _session;

        public HughesSpeedTest AdditionalQueryParameters { get; set; }

        public Dictionary<string, string> SortOptions { get; set; }

        public DateTime StartPeriod { get; set; }

        public DateTime EndPeriod { get; set; }

        public HughesSpeedTestDataSource(ISession session)
        {
            _session = session;
        }

        public List<HughesSpeedTest> GetHughesSpeedTests(string serialNum)
        {
            IQuery query = _session.GetNamedQuery("select-hughes-speed-tests-by-serialnum");
            query.SetString("serialNum", serialNum);
            query.SetResultTransformer(Transformers.AliasToBean(typeof(HughesSpeedTest)));
            var list = query.List<HughesSpeedTest>().ToList<HughesSpeedTest>();
            FormatHughesSpeedTestList(list);
            return list;
        }

        /// <summary>
        /// Получить тесты по фильтру в AdditionalQueryParameters, StartPeriod и EndPeriod, SortingOptions
        /// без учёта паджинации, то есть все данные сразу.
        /// </summary>
        /// <returns></returns>
        public List<HughesSpeedTest> GetHughesSpeedTests()
        {
            return GetPageData(0, 0);
        }

        /// <summary>
        /// дополнительное форматирование списка тестов
        /// </summary>
        /// <param name="testList"></param>
        private void FormatHughesSpeedTestList(List<HughesSpeedTest> testList)
        {
            foreach (HughesSpeedTest speedTest in testList)
            {
                //формирование URL дамп-файлов
                speedTest.upload_dump = ClientInfoWcfService.Configuration.Parameters.Instance.Config.HughesSpeedTestSettings.DumpFileStorageUrl + speedTest.upload_dump;
                //конвертирование в килобиты
                speedTest.medium_speed_download = Math.Round(speedTest.medium_speed_download * 8 / 1000, 2);
                speedTest.medium_speed_upload = Math.Round(speedTest.medium_speed_upload * 8 / 1000, 2);
            }
        }

        public List<HughesSpeedTestDetail> GetHughesSpeedTestDetails(int testId)
        {
            IQuery query = _session.GetNamedQuery("select-hughes-speed-test-details-by-test-id");
            query.SetInt32("ptest_id", testId);
            query.SetResultTransformer(Transformers.AliasToBean(typeof(HughesSpeedTestDetail)));
            var list = query.List<HughesSpeedTestDetail>().ToList<HughesSpeedTestDetail>();

            foreach (HughesSpeedTestDetail speedTestDetail in list)
            {
                //конвертирование в килобиты
                speedTestDetail.traffic_amount_in = Math.Round(speedTestDetail.traffic_amount_in * 8 / 1000, 2);
                speedTestDetail.traffic_amount_out = Math.Round(speedTestDetail.traffic_amount_out * 8 / 1000, 2);
            }

            if (list.Count > 0)
                return list;
            else
                return null;
        }

        private IQuery addDateFilterAndSortingToQuery(IQuery sourceQuery)
        {
            string modifiedQueryString = sourceQuery.QueryString;

            bool dateFilterExists = false;
            //добавляем фильтр по дате
            if (StartPeriod != DateTime.MinValue && EndPeriod != DateTime.MinValue)
            {
                dateFilterExists = true;
                modifiedQueryString += " and test_datetime between :startPeriod and :endPeriod";
            }

            //добавляем опции сортировки
            if (SortOptions != null)
            {
                modifiedQueryString += GetSortQueryEnding(SortOptions);
            }

            ISQLQuery modifiedQuery = _session.CreateSQLQuery(modifiedQueryString);
            FieldInfo returnTypesFi = sourceQuery.GetType().GetField("queryReturns", BindingFlags.NonPublic | BindingFlags.Instance);
            //SQLQueryScalarReturn
            ArrayList returns = (ArrayList)returnTypesFi.GetValue(sourceQuery);
            foreach (object value in returns)
            {
                modifiedQuery.AddScalar(((SQLQueryScalarReturn)value).ColumnAlias, ((SQLQueryScalarReturn)value).Type);
            }

            if (dateFilterExists)
            {
                modifiedQuery.SetDateTime("startPeriod", StartPeriod);
                modifiedQuery.SetDateTime("endPeriod", EndPeriod);
            }

            return modifiedQuery;
        }

        /// <summary>
        /// Получить тесты по фильтру в AdditionalQueryParameters, StartPeriod и EndPeriod, SortingOptions
        /// с учётом паджинации, а если pageSize == 0, то без учета паджинации, то есть все данные разом
        /// </summary>
        /// <param name="startIndex"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        protected override List<HughesSpeedTest> GetPageData(int startIndex, int pageSize)
        {
            IQuery query = _session.GetNamedQuery("select-hughes-speed-tests");
            var parameters = CopyParameters(AdditionalQueryParameters);
            query = QueryHelper.GetClausedQuery(_session, query, parameters);
            query = addDateFilterAndSortingToQuery(query);
            if (pageSize != 0)
            {
                query.SetFirstResult(startIndex);
                query.SetMaxResults(pageSize);
            }
            query.SetResultTransformer(Transformers.AliasToBean(typeof(HughesSpeedTest)));
            var list = query.List<HughesSpeedTest>().ToList<HughesSpeedTest>();
            FormatHughesSpeedTestList(list);
            return list;
        }

        private static HughesSpeedTest CopyParameters(HughesSpeedTest parameters)
        {
            HughesSpeedTest clone = new HughesSpeedTest();
            foreach (var property in typeof(HughesSpeedTest).GetProperties())
            {
                property.SetValue(clone, property.GetValue(parameters, null), null);
            }
            return clone;
        }

        private static string GetSortQueryEnding(Dictionary<string, string> sortOptions)
        {
            StringBuilder ending = new StringBuilder();

            if (sortOptions == null || sortOptions.Keys.Count == 0)
                return null;

            const string orderByClause = " order by ";
            ending.Append(orderByClause);

            foreach (string propertyName in sortOptions.Keys)
            {
                if (sortOptions[propertyName] == "ASC")
                {
                    ending.AppendFormat(" {0} ASC, ", propertyName);
                }
                else if (sortOptions[propertyName] == "DESC")
                {
                    ending.AppendFormat(" {0} DESC, ", propertyName);
                }
            }

            if (ending.ToString() == orderByClause)
                return null;
            else
            {
                ending.Remove(ending.Length - 2, 2); //удаляем последнюю запятую и пробел
                return ending.ToString();
            }
        }

        protected override int GetRecordCount()
        {
            IQuery query = _session.GetNamedQuery("select-hughes-speed-tests");
            var parameters = CopyParameters(AdditionalQueryParameters);
            query = QueryHelper.GetClausedQueryForCount(_session, query, parameters);
            query = addDateFilterAndSortingToQuery(query);
            return query.List<int>()[0];
        }

        public List<SerialNumberReportEntry> GetSerialNumberReportData(DateTime startPeriod, DateTime endPeriod)
        {
            IQuery query = _session.GetNamedQuery("select-hughes-speed-test-report-serialnum");
            query.SetDateTime("startPeriod", startPeriod);
            query.SetDateTime("endPeriod", endPeriod);
            query.SetResultTransformer(Transformers.AliasToBean(typeof(SerialNumberReportEntry)));
            var list = query.List<SerialNumberReportEntry>().ToList<SerialNumberReportEntry>();

            foreach (SerialNumberReportEntry entry in list)
            {
                //конвертирование в килобиты
                entry.mediumDownloadSpeed = Math.Round(entry.mediumDownloadSpeed * 8 / 1000, 2);
                entry.mediumUploadSpeed = Math.Round(entry.mediumUploadSpeed * 8 / 1000, 2);
            }

            return list;
        }

        public List<AddressReportEntry> GetAddressReportData(DateTime startPeriod, DateTime endPeriod)
        {
            IQuery query = _session.GetNamedQuery("select-hughes-speed-test-report-abonent-ip");
            query.SetDateTime("startPeriod", startPeriod);
            query.SetDateTime("endPeriod", endPeriod);
            query.SetResultTransformer(Transformers.AliasToBean(typeof(AddressReportEntry)));
            var list = query.List<AddressReportEntry>().ToList<AddressReportEntry>();

            foreach (AddressReportEntry entry in list)
            {
                //конвертирование в килобиты
                entry.mediumDownloadSpeed = Math.Round(entry.mediumDownloadSpeed * 8 / 1000, 2);
                entry.mediumUploadSpeed = Math.Round(entry.mediumUploadSpeed * 8 / 1000, 2);
            }

            return list;
        }

        public List<HourReportEntry> GetHourReportData(DateTime startPeriod, DateTime endPeriod)
        {
            IQuery query = _session.GetNamedQuery("select-hughes-speed-test-report-hours");
            query.SetDateTime("startPeriod", startPeriod);
            query.SetDateTime("endPeriod", endPeriod);
            query.SetResultTransformer(Transformers.AliasToBean(typeof(HourReportEntry)));
            var list = query.List<HourReportEntry>().ToList<HourReportEntry>();

            foreach (HourReportEntry entry in list)
            {
                //конвертирование в килобиты
                entry.mediumDownloadSpeed = Math.Round(entry.mediumDownloadSpeed * 8 / 1000, 2);
                entry.mediumUploadSpeed = Math.Round(entry.mediumUploadSpeed * 8 / 1000, 2);
            }

            return list;
        }
    }
}