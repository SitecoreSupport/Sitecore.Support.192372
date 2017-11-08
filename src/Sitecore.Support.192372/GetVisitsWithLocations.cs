using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Sitecore.Cintel.Reporting.Processors;
using Sitecore.Support.Cintel.Utility;
using Sitecore.XConnect;
using Sitecore.XConnect.Collection.Model;
using Sitecore.Cintel.Reporting.ReportingServerDatasource;
using Sitecore.Cintel.Reporting;

namespace Sitecore.Support.Cintel.Reporting.ReportingServerDatasource.Visits
{
    /// <summary>
    /// Gets the visists from the Xdb for the given Contact
    /// </summary>
    public class GetVisitsWithLocations : ReportProcessorBase
    {

        /// <summary>
        /// Execution step of the Visits pipeline.
        /// </summary>
        /// <param name="args"></param>
        public override void Process(ReportProcessorArgs args)
        {
            DataTable interactions = CreateTableWithSchema();
            Guid contactId = args.ReportParameters.ContactId;
            Guid InteractionId;
            if (Guid.TryParse(args.ReportParameters.ViewEntityId, out InteractionId))
            {
                this.GetTableFromContactXconnect(interactions, args.ReportParameters.ContactId, InteractionId);
            }
            else
            {
                this.GetTableFromContactXconnect(interactions, contactId);
            }

            args.QueryResult = interactions;
        }

        private DataTable CreateTableWithSchema()
        {
            var resultTable = new DataTable();

            resultTable.Columns.AddRange(new[]
            {
        new DataColumn(XConnectFields.Interaction.ContactId, typeof(Guid)),
        new DataColumn(XConnectFields.Interaction.Id, typeof(Guid)),
        new DataColumn(XConnectFields.Interaction.ChannelId, typeof(Guid)),
        new DataColumn(XConnectFields.Interaction.StartDate, typeof(DateTime)),
        new DataColumn(XConnectFields.Interaction.EndDate, typeof(DateTime)),
        new DataColumn(XConnectFields.Interaction.SessionId, typeof(Guid)),
        new DataColumn(XConnectFields.Interaction.CampaignId, typeof(Guid)),
        new DataColumn(XConnectFields.Interaction.ContactVisitIndex, typeof(int)),
        new DataColumn(XConnectFields.Interaction.DeviceId, typeof(Guid)),
        new DataColumn(XConnectFields.Interaction.LocationId, typeof(Guid)),
        new DataColumn(XConnectFields.Interaction.UserAgent, typeof(string)),
        new DataColumn(XConnectFields.Interaction.SiteName, typeof(string)),
        new DataColumn(XConnectFields.Interaction.Value, typeof(int)),
        new DataColumn(XConnectFields.Interaction.PageCount, typeof(int)),
        new DataColumn(XConnectFields.Interaction.VisitIp, typeof(string)),
        new DataColumn(XConnectFields.Interaction.Keywords, typeof(string)),
        new DataColumn(XConnectFields.Interaction.ReferringSite, typeof(string)),
        new DataColumn(XConnectFields.GeoData.BusinessName, typeof(string)),
        new DataColumn(XConnectFields.GeoData.City, typeof(string)),
        new DataColumn(XConnectFields.GeoData.Region, typeof(string)),
        new DataColumn(XConnectFields.GeoData.Country, typeof(string)),
      });

            return resultTable;
        }

        private void GetTableFromContactXconnect(DataTable rawTable, Guid contactID, Guid? interactionID = null)
        {
            string[] interactionFacet = { IpInfo.DefaultFacetKey, WebVisit.DefaultFacetKey, UserAgentInfo.DefaultFacetKey };

            var options = new ContactExpandOptions() { Interactions = new RelatedInteractionsExpandOptions(interactionFacet) { Limit = int.MaxValue, StartDateTime = DateTime.MinValue } };
            var contact = XdbContactServiceHelper.GetContactByOptions(contactID, options);
            List<Interaction> interactions = contact.Interactions.OrderByDescending(p => p.StartDateTime).ToList();
            int index = 1;
            if (interactionID.HasValue)
            {
                var interaction = interactions.FirstOrDefault(p => p.Id == interactionID.Value);
                index = interactions.IndexOf(interaction) + 1;
                FillTableWithRow(rawTable, interaction, index);
            }
            else
            {
                foreach (var curInteraction in interactions)
                {
                    index = interactions.IndexOf(curInteraction) + 1;
                    FillTableWithRow(rawTable, curInteraction, index);
                }
            }
        }

        private void FillTableWithRow(DataTable rawTable, Interaction curInteraction, int index = 1)
        {
            var webVisit = curInteraction.WebVisit();
            var ipInfo = curInteraction.IpInfo();
            var viewList = curInteraction.Events.OfType<PageViewEvent>().ToList();
            var viewsCount = viewList.Count;
            var row = rawTable.NewRow();
            row[XConnectFields.Interaction.ContactId] = curInteraction.Contact.Id;
            row[XConnectFields.Interaction.Id] = curInteraction.Id;
            row[XConnectFields.Interaction.ChannelId] = curInteraction.ChannelId;
            row[XConnectFields.Interaction.StartDate] = curInteraction.StartDateTime;
            row[XConnectFields.Interaction.EndDate] = curInteraction.EndDateTime;
            row[XConnectFields.Interaction.SessionId] = Guid.Empty; //ToDo:change to xConnecct-Session Id not ready;
            if (curInteraction.CampaignId.HasValue)
            {
                row[XConnectFields.Interaction.CampaignId] = curInteraction.CampaignId;
            }
            row[XConnectFields.Interaction.ContactVisitIndex] = index;
            row[XConnectFields.Interaction.DeviceId] = curInteraction.DeviceProfile.Id;
            row[XConnectFields.Interaction.LocationId] = Guid.Empty;//ToDo:change to xConnecct-Location Id Not available yet - wait for PI5;
            row[XConnectFields.Interaction.UserAgent] = curInteraction.UserAgent;
            row[XConnectFields.Interaction.SiteName] = webVisit.SiteName;
            row[XConnectFields.Interaction.Value] = curInteraction.EngagementValue;
            row[XConnectFields.Interaction.PageCount] = viewsCount;
            row[XConnectFields.Interaction.VisitIp] = ipInfo.IpAddress.ToString();
            row[XConnectFields.Interaction.Keywords] = webVisit.SearchKeywords;
            if (!string.IsNullOrEmpty(webVisit.Referrer))
            {
                Uri url = new Uri(webVisit.Referrer);
                row[XConnectFields.Interaction.ReferringSite] = url.Host;
            }
            row[XConnectFields.GeoData.BusinessName] = ipInfo.BusinessName;
            row[XConnectFields.GeoData.City] = ipInfo.City;
            row[XConnectFields.GeoData.Region] = ipInfo.Region;
            row[XConnectFields.GeoData.Country] = ipInfo.Country;

            rawTable.Rows.Add(row);
        }
    }
}
