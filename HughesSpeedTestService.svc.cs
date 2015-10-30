using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using ClientInfoWcfService.Entities;
using NHibernate;
using ClientInfoWcfService.Common;
using ClientInfoWcfService.DAL.DataSources;
using System.ServiceModel.Activation;
using ClientInfoWcfService.Reports;
using ClientInfoWcfService.Entities.HughesSpeedTest;
using ClientInfoWcfService.ServiceInterfaces;

namespace ClientInfoWcfService.Services
{
    [AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Allowed)]
    [ServiceBehavior(IncludeExceptionDetailInFaults = true)]
    public class HughesSpeedTestService : IHughesSpeedTestService
    {
        private HughesSpeedTestDataSource _hughesSpeedTestDataSource;

        public HughesSpeedTestService()
        {
            ServicesHelper.Connect();

            ISession session = ServicesHelper.GetNewSession();

            _hughesSpeedTestDataSource = new HughesSpeedTestDataSource(session);
        }

        /// <summary>
        /// Получить подробную статистику для теста скорости
        /// </summary>
        /// <param name="testId">Id теста</param>
        /// <returns></returns>
        public List<HughesSpeedTestDetail> GetHughesSpeedTestDetails(int testId)
        {
            try
            {
                return _hughesSpeedTestDataSource.GetHughesSpeedTestDetails(testId);
            }
            catch (Exception exception)
            {
                string additionalInfo = string.Format("Ошибка получения HughesSpeedTestDetail для теста с testId = {0}", testId);
                ServiceException serviceException = new ServiceException(additionalInfo, exception);
                ServicesHelper.WriteErrorToDisk(typeof(ContractService), serviceException);
                throw serviceException;
            }
        }
        
        public List<HughesSpeedTest> GetHughesSpeedTests(HughesSpeedTest filter, int pageNumber, int pageSize)
        {
            try
            {
                _hughesSpeedTestDataSource.AdditionalQueryParameters = filter;
                _hughesSpeedTestDataSource.CurrentPage = pageNumber;
                _hughesSpeedTestDataSource.PageSize = pageSize;
                return _hughesSpeedTestDataSource.PageData;
            }
            catch (Exception exception)
            {
                ServiceException serviceException = new ServiceException(exception);
                ServicesHelper.WriteErrorToDisk(typeof(HughesSpeedTestService), serviceException);
                throw serviceException;
            }
        }

        public List<HughesSpeedTest> GetHughesSpeedTestsSorted(
            HughesSpeedTest filter, 
            int pageNumber, 
            int pageSize, 
            Dictionary<string, string> sortOptions,
            DateTime startPeriod, 
            DateTime endPeriod)
        {
            _hughesSpeedTestDataSource.SortOptions = sortOptions;
            _hughesSpeedTestDataSource.StartPeriod = startPeriod;
            _hughesSpeedTestDataSource.EndPeriod = endPeriod;
            return GetHughesSpeedTests(filter, pageNumber, pageSize);
        }

        public int GetHughesSpeedTestsCount(
            HughesSpeedTest filter,
            DateTime startPeriod, 
            DateTime endPeriod)
        {
            try
            {
                _hughesSpeedTestDataSource.StartPeriod = startPeriod;
                _hughesSpeedTestDataSource.EndPeriod = endPeriod;
                _hughesSpeedTestDataSource.AdditionalQueryParameters = filter;
                return _hughesSpeedTestDataSource.TotalRecordCount;
            }
            catch (Exception exception)
            {
                ServiceException serviceException = new ServiceException(exception);
                ServicesHelper.WriteErrorToDisk(typeof(HughesSpeedTestService), serviceException);
                throw serviceException;
            }
        }

        public string GenerateReport(DateTime startPeriod, DateTime endPeriod)
        {
            string fileName = @"c:\Unip Reports\HughesSpeedTestReports\HughesSpeedTestReport.xlsx";
            HughesSpeedTestReport report = new HughesSpeedTestReport();
            List<SerialNumberReportEntry> serialNumberReportEntries = _hughesSpeedTestDataSource.GetSerialNumberReportData(startPeriod, endPeriod);
            List<AddressReportEntry> addressReportEntries = _hughesSpeedTestDataSource.GetAddressReportData(startPeriod, endPeriod);
            List<HourReportEntry> hourReportEntries = _hughesSpeedTestDataSource.GetHourReportData(startPeriod, endPeriod);
            report.SaveHughesSpeedTestReport(fileName, startPeriod, endPeriod, serialNumberReportEntries, addressReportEntries, hourReportEntries);
            return fileName;
        }

        public string GenerateSearchResultExcelFile(
            HughesSpeedTest filter,
            Dictionary<string, string> sortOptions,
            DateTime startPeriod,
            DateTime endPeriod)
        {
            string fileName = @"c:\Unip Reports\HughesSpeedTestReports\HughesSpeedTestSearchResults.xlsx";
            HughesSpeedTestReport report = new HughesSpeedTestReport();
            _hughesSpeedTestDataSource.AdditionalQueryParameters = filter;
            _hughesSpeedTestDataSource.SortOptions = sortOptions;
            _hughesSpeedTestDataSource.StartPeriod = startPeriod;
            _hughesSpeedTestDataSource.EndPeriod = endPeriod;
            List<HughesSpeedTest> searchResultsList = _hughesSpeedTestDataSource.GetHughesSpeedTests();
            report.SaveSearchResults(fileName, searchResultsList);
            return fileName;
        }
    }
}
