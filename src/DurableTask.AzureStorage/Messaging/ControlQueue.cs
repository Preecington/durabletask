//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

namespace DurableTask.AzureStorage.Messaging
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using DurableTask.AzureStorage.Monitoring;
    using Microsoft.WindowsAzure.Storage.Queue;

    class ControlQueue : TaskHubQueue, IDisposable
    {
        readonly CancellationTokenSource releaseTokenSource;
        readonly CancellationToken releaseCancellationToken;

        public ControlQueue(
            CloudQueue storageQueue,
            AzureStorageOrchestrationServiceSettings settings,
            AzureStorageOrchestrationServiceStats stats,
            MessageManager messageManager)
            : base(storageQueue, settings, stats, messageManager)
        {
            this.releaseTokenSource = new CancellationTokenSource();
            this.releaseCancellationToken = this.releaseTokenSource.Token;
        }

        public bool IsReleased { get; private set; }

        protected override QueueRequestOptions QueueRequestOptions => this.settings.ControlQueueRequestOptions;

        protected override TimeSpan MessageVisibilityTimeout => this.settings.ControlQueueVisibilityTimeout;

        public async Task<IEnumerable<MessageData>> GetMessagesAsync(CancellationToken cancellationToken)
        {
            using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(this.releaseCancellationToken, cancellationToken))
            {
                while (!linkedCts.IsCancellationRequested)
                {
                    // Pause dequeuing if the total number of locked messages gets too high.
                    if (this.stats.PendingOrchestratorMessages.Value >= this.settings.ControlQueueBufferThreshold)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1));
                        continue;
                    }

                    try
                    {
                        IEnumerable<CloudQueueMessage> batch = await this.storageQueue.GetMessagesAsync(
                            this.settings.ControlQueueBatchSize,
                            this.settings.ControlQueueVisibilityTimeout,
                            this.settings.ControlQueueRequestOptions,
                            null /* operationContext */,
                            linkedCts.Token);

                        this.stats.StorageRequests.Increment();

                        if (!batch.Any())
                        {
                            await this.backoffHelper.WaitAsync(linkedCts.Token);
                            continue;
                        }

                        var batchMessages = new ConcurrentBag<MessageData>();
                        await batch.ParallelForEachAsync(async delegate (CloudQueueMessage queueMessage)
                        {
                            this.stats.MessagesRead.Increment();

                            MessageData messageData = await this.messageManager.DeserializeQueueMessageAsync(
                                queueMessage,
                                this.storageQueue.Name);

                            AzureStorageOrchestrationService.TraceMessageReceived(
                                messageData,
                                this.storageAccountName,
                                this.settings.TaskHubName);

                            batchMessages.Add(messageData);
                        });

                        this.backoffHelper.Reset();

                        return batchMessages;
                    }
                    catch (Exception e)
                    {
                        if (!linkedCts.IsCancellationRequested)
                        {
                            AnalyticsEventSource.Log.MessageFailure(
                                this.storageAccountName,
                                this.settings.TaskHubName,
                                string.Empty,
                                string.Empty,
                                this.storageQueue.Name,
                                string.Empty,
                                e.ToString(),
                                Utils.ExtensionVersion);

                            await this.backoffHelper.WaitAsync(linkedCts.Token);
                        }
                    }
                }

                this.IsReleased = true;
                return Enumerable.Empty<MessageData>();
            }
        }

        public void Release()
        {
            this.releaseTokenSource.Cancel();
        }

        public virtual void Dispose()
        {
            this.releaseTokenSource.Dispose();
        }
    }
}
