//------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
//------------------------------------------------------------

using System;
using System.IO;
using Newtonsoft.Json;
using System.IO.Compression;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.Functions.Extensions.Workflows;

namespace logistics.fa
{
    public class FileUnzipFA_MSI
    {
        [FunctionName("FileUnzipFA_MSI")]
        public async Task<TaskStatus> Run(
            [WorkflowActionTrigger] string SourceBlobUrl,
            string DestinationBlobUrl,
            string SourceContainerName,
            string DestinationContainerName,
            string SourceBlobName,
            string DestinationFolderName,
            string TopicName,
            string CorrelationId,
            string IntId,
            string EventType,
            string ZipFileName)
        {
            var currentTaskStatus = new TaskStatus
            {
                CurrentTaskStatus = "Starting the file unzip process."
            };

            try
            {
                await FileUnzipAndUploadToBlob(
                    SourceBlobUrl, DestinationBlobUrl,
                    SourceContainerName, DestinationContainerName,
                    SourceBlobName, DestinationFolderName,
                    TopicName, CorrelationId, IntId, EventType, ZipFileName);

                currentTaskStatus.CurrentTaskStatus = "Files unzipped and uploaded to destination blob successfully.";
            }
            catch (Exception ex)
            {
                currentTaskStatus.CurrentTaskStatus = $"Error: {ex.Message}";
            }

            return currentTaskStatus;
        }

        private static async Task FileUnzipAndUploadToBlob(
            string sourceBlobUrl, string destinationBlobUrl,
            string sourceContainerName, string destinationContainerName,
            string sourceBlobName, string destinationFolderName,
            string topicName, string correlationId, string intId, string eventType, string zipFileName)
        {
            var credential = new DefaultAzureCredential();
            var sourceBlobServiceClient = new BlobServiceClient(new Uri(sourceBlobUrl), credential);
            var sourceBlobContainerClient = sourceBlobServiceClient.GetBlobContainerClient(sourceContainerName);

            if (!await sourceBlobContainerClient.ExistsAsync())
            {
                throw new DirectoryNotFoundException($"Source blob container '{sourceContainerName}' not found.");
            }

            var sourceBlobClient = sourceBlobContainerClient.GetBlobClient(sourceBlobName);

            if (!await sourceBlobClient.ExistsAsync())
            {
                throw new FileNotFoundException($"Source blob '{sourceBlobName}' not found.");
            }

            var downloadResponse = await sourceBlobClient.DownloadAsync();
            using (var memoryStream = new MemoryStream())
            {
                await downloadResponse.Value.Content.CopyToAsync(memoryStream);
                memoryStream.Position = 0;

                using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Read))
                {
                    var serviceBusClient = new ServiceBusClient(topicName, credential);
                    var sender = serviceBusClient.CreateSender(topicName);

                    foreach (var entry in archive.Entries)
                    {
                        if (entry.FullName.EndsWith("/") || string.IsNullOrEmpty(entry.Name))
                        {
                            continue;
                        }

                        var destinationBlobName = Path.Combine(destinationFolderName, entry.Name).Replace("\\", "/");
                        var destinationBlobServiceClient = new BlobServiceClient(new Uri(destinationBlobUrl), credential);
                        var destinationContainerClient = destinationBlobServiceClient.GetBlobContainerClient(destinationContainerName);
                        var destinationBlobClient = destinationContainerClient.GetBlobClient(destinationBlobName);

                        using (var entryStream = entry.Open())
                        {
                            await destinationBlobClient.UploadAsync(entryStream, true);
                        }

                        var transactionId = "TRANS" + Guid.NewGuid().ToString().ToUpper();
                        var messageBody = JsonConvert.SerializeObject(new
                        {
                            IntId = intId,
                            TransactionId = transactionId,
                            CorrelationId = correlationId,
                            FileName = entry.Name,
                            ArchiveBlobFullPath = sourceBlobName,
                            ZipFileName = zipFileName,
                            EventType = eventType,
                            UnzipBlobFullPath = destinationBlobName
                        });

                        var message = new ServiceBusMessage(System.Text.Encoding.UTF8.GetBytes(messageBody))
                        {
                            MessageId = Guid.NewGuid().ToString(),
                            ApplicationProperties =
                            {
                                { "IntId", intId },
                                { "TransactionId", transactionId },
                                { "CorrelationId", correlationId },
                                { "FileName", entry.Name },
                                { "ArchiveBlobFullPath", sourceBlobName },
                                { "ZipFileName", zipFileName },
                                { "EventType", eventType },
                                { "UnzipBlobFullPath", destinationBlobName }
                            }
                        };

                        await sender.SendMessageAsync(message);
                    }

                    await sender.CloseAsync();
                    await serviceBusClient.DisposeAsync();
                }
            }
        }

        public class TaskStatus
        {
            public string CurrentTaskStatus { get; set; }
        }
    }
}