using System.Collections.Generic;
using System.Collections.ObjectModel;
using Sitecore.XConnect;
using Sitecore.XConnect.Operations;

namespace Sitecore.Support.Cintel.Utility
{
    using System;
    using System.Threading.Tasks;
    using Sitecore.XConnect.Client;
    using Sitecore.XConnect.Client.Configuration;
    using Sitecore.Cintel.ContactService;
    using XConnect.Collection.Model;

    internal class XdbContactServiceHelper
    {
        internal static Contact GetContactByOptions(Guid contactId, ExpandOptions options = null)
        {
            using (XConnectClient client = SitecoreXConnectClientConfiguration.GetClient())
            {
                if (options == null)
                {
                    options = new ContactExpandOptions()
                    {
                        Interactions = new RelatedInteractionsExpandOptions(IpInfo.DefaultFacetKey)
                    };
                }
                var reference = new ContactReference(contactId);

                Contact contact = client.Get<Contact>(reference, options);


                if (contact == null)
                {
                    throw new ContactNotFoundException(string.Format("No Contact with id [{0}] found", contactId));
                }
                return contact;
            }

        }

    }
}