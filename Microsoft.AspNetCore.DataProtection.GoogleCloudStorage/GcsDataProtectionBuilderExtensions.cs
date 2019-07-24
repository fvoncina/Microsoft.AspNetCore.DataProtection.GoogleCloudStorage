using System;
using System.Collections.Generic;
using System.Text;
using Google.Cloud.Storage.V1;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.AspNetCore.DataProtection.GoogleCloudStorage
{
    public static class GcsDataProtectionBuilderExtensions
    {

        public static IDataProtectionBuilder PersistKeysToGcs(this IDataProtectionBuilder builder, Func<StorageClient> gcsClientFactory, string bucket, string name = "keys")
        {
            if (builder == null)
            {
                throw new ArgumentNullException(nameof(builder));
            }
            if (gcsClientFactory == null)
            {
                throw new ArgumentNullException(nameof(gcsClientFactory));
            }
            if (string.IsNullOrEmpty(bucket))
            {
                throw new ArgumentNullException(nameof(bucket));
            }
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            builder.Services.Configure<KeyManagementOptions>(options =>
            {
                options.XmlRepository = new GcsXmlRepository(gcsClientFactory, bucket, name);
            });

            return builder;
        }


    }
}
