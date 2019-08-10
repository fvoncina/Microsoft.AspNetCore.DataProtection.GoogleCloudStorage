using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Google.Cloud.Storage.V1;
using Microsoft.AspNetCore.DataProtection.Repositories;

namespace Microsoft.AspNetCore.DataProtection.GoogleCloudStorage
{
    public sealed class GcsXmlRepository : IXmlRepository
    {
        private const int ConflictMaxRetries = 5;
        private static readonly TimeSpan ConflictBackoffPeriod = TimeSpan.FromMilliseconds(200);

        private static readonly XName RepositoryElementName = "repository";

        private readonly Func<StorageClient> _gcsClientFactory;
        private readonly Random _random;
        private BlobData _cachedBlobData;
        private readonly string _bucketName;
        private readonly string _name;

        public GcsXmlRepository(Func<StorageClient> gcsClientFactory, string bucketName, string name = "keys")
        {
            _gcsClientFactory = gcsClientFactory ?? throw new ArgumentNullException(nameof(gcsClientFactory));
            _bucketName = bucketName ?? throw new ArgumentNullException(nameof(bucketName));
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _random = new Random();
        }

        public IReadOnlyCollection<XElement> GetAllElements()
        {
            var storageClient = CreateFreshStorageClient();
            var elements = Task.Run(() => GetAllElementsAsync(storageClient)).GetAwaiter().GetResult();
            return new ReadOnlyCollection<XElement>(elements);
        }

        public void StoreElement(XElement element, string friendlyName)
        {
            if (element == null)
            {
                throw new ArgumentNullException(nameof(element));
            }

            var storageClient = CreateFreshStorageClient();
            Task.Run(() => StoreElementAsync(storageClient, element)).GetAwaiter().GetResult();
        }

        private XDocument CreateDocumentFromBlob(byte[] blob)
        {
            using (var memoryStream = new MemoryStream(blob))
            {
                var xmlReaderSettings = new XmlReaderSettings()
                {
                    DtdProcessing = DtdProcessing.Prohibit,
                    IgnoreProcessingInstructions = true
                };

                using (var xmlReader = XmlReader.Create(memoryStream, xmlReaderSettings))
                {
                    return XDocument.Load(xmlReader);
                }
            }
        }

        private StorageClient CreateFreshStorageClient()
        {
            var storageClient = _gcsClientFactory();
            if (storageClient == null)
            {
                throw new InvalidOperationException("The ICloudBlob factory method returned null.");
            }

            return storageClient;
        }

        private async Task<IList<XElement>> GetAllElementsAsync(StorageClient storageClient)
        {
            var data = await GetLatestDataAsync(storageClient);

            if (data == null)
            {
                return new XElement[0];
            }
            var doc = CreateDocumentFromBlob(data.BlobContents);
            return doc.Root.Elements().ToList();
        }

        private async Task<BlobData> GetLatestDataAsync(StorageClient storageClient)
        {
            var latestCachedData = Volatile.Read(ref _cachedBlobData); // local ref so field isn't mutated under our feet
            try
            {
                using (var memoryStream = new MemoryStream())
                {
                    var obj = await storageClient.GetObjectAsync(_bucketName, _name);
                    await storageClient.DownloadObjectAsync(obj, memoryStream);
                    latestCachedData = new BlobData()
                    {
                        BlobContents = memoryStream.ToArray(),
                        ETag = obj.ETag
                    };
                }
                Volatile.Write(ref _cachedBlobData, latestCachedData);
            }
            catch
            {
                latestCachedData = null;
                Volatile.Write(ref _cachedBlobData, latestCachedData);
            }
            return latestCachedData;
        }

        private int GetRandomizedBackoffPeriod()
        {
            var multiplier = 0.8 + ( _random.NextDouble() * 0.2 );
            return (int)( multiplier * ConflictBackoffPeriod.Ticks );
        }

        private async Task StoreElementAsync(StorageClient storageClient, XElement element)
        {
            ExceptionDispatchInfo lastError = null;

            for (var i = 0; i < ConflictMaxRetries; i++)
            {
                if (i > 1)
                {
                    await Task.Delay(GetRandomizedBackoffPeriod());
                }

                if (i > 0)
                {
                    await GetLatestDataAsync(storageClient);
                }

                var latestData = Volatile.Read(ref _cachedBlobData);
                var doc = ( latestData != null )
                    ? CreateDocumentFromBlob(latestData.BlobContents)
                    : new XDocument(new XElement(RepositoryElementName));
                doc.Root.Add(element);

                using (var serializedDoc = new MemoryStream())
                {
                    doc.Save(serializedDoc, SaveOptions.DisableFormatting);

                    try
                    {
                        serializedDoc.Seek(0, SeekOrigin.Begin);

                        var obj = await storageClient.UploadObjectAsync(_bucketName, _name, "application/xml", serializedDoc);

                        Volatile.Write(ref _cachedBlobData, new BlobData()
                        {
                            BlobContents = serializedDoc.ToArray(),
                            ETag = obj.ETag // was updated by Upload routine
                        });

                        return;
                    }
                    catch (Exception ex)
                    {
                        lastError = ExceptionDispatchInfo.Capture(ex);
                    }
                }
            }

            lastError.Throw();
        }

        private sealed class BlobData
        {

            internal byte[] BlobContents { get; set; }

            internal string ETag { get; set; }

        }
    }
}
