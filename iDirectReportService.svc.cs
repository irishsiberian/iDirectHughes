using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using ClientInfoWcfService.ServiceInterfaces;
using System.ServiceModel.Activation;
using ClientInfoWcfService.Entities.iDirectReports;
using ClientInfoWcfService.Common;
using ClientInfoWcfService.DAL.DataSources.IDirect;
using NHibernate;

namespace ClientInfoWcfService.Services
{
    [AspNetCompatibilityRequirements(RequirementsMode = AspNetCompatibilityRequirementsMode.Allowed)]
    [ServiceBehavior(IncludeExceptionDetailInFaults = true)]
    public class iDirectReportService : IiDirectReportService
    {
        private iDirectReportDataSource _iDirectReportDataSource;

        public iDirectReportService()
        {
            _iDirectReportDataSource = new iDirectReportDataSource();
        }

        #region IiDirectReportService Members

        public List<Network> GetTerminalsTree()
        {
            try
            {
                return _iDirectReportDataSource.GetTerminalsTree();
            }
            catch (Exception exception)
            {
                string additionalInfo = "Ошибка получения Networks";
                ServiceException serviceException = new ServiceException(additionalInfo, exception);
                ServicesHelper.WriteErrorToDisk(typeof(iDirectReportService), serviceException);
                throw serviceException;
            }
        }

        public List<Entities.iDirectReports.ReportEntry> GetReport(DateTime startPeriod, DateTime endPeriod, List<uint> terminalIds)
        {
            try
            {
                return _iDirectReportDataSource.GetReport(startPeriod, endPeriod, terminalIds);
            }
            catch (Exception exception)
            {
                string additionalInfo = "Ошибка получения Networks";
                ServiceException serviceException = new ServiceException(additionalInfo, exception);
                ServicesHelper.WriteErrorToDisk(typeof(iDirectReportService), serviceException);
                throw serviceException;
            }
        }

        #endregion
    }
}
