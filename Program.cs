using Azure.Messaging.EventHubs.Consumer;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace MSAzureEventHubReceiver
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string connectionString = "PrimaryOrSecondary Connection String";
            string eventHubName = "EventHub Name";

            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            try
            {
                await using (EventHubConsumerClient consumer = new EventHubConsumerClient(consumerGroup, connectionString, eventHubName))
                {
                    //Task t1 = Task.Run(async () =>
                    //{
                    //    try
                    //    {
                    //        using CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                    //        cancellationTokenSource.CancelAfter(TimeSpan.FromHours(1));

                    //        await foreach (PartitionEvent receivedEvent in consumer.ReadEventsAsync(cancellationTokenSource.Token))
                    //        {
                    //            Console.WriteLine($"T1 ReceivedEvent : {receivedEvent.Partition.PartitionId} | {receivedEvent.Data.EventBody}");
                    //        }
                    //    }
                    //    catch (Exception ex)
                    //    {
                    //        Console.WriteLine($"Exception in T1 {ex.ToString()}");
                    //    }
                    //});

                    Task t2 = Task.Run(async () =>
                    {
                        try
                        {
                            using CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                            cancellationTokenSource.CancelAfter(TimeSpan.FromHours(1));

                            await foreach (PartitionEvent receivedEvent in consumer.ReadEventsAsync(cancellationTokenSource.Token))
                            {
                                Console.WriteLine($"T2 ReceivedEvent : {receivedEvent.Partition.PartitionId} | {receivedEvent.Data.EventBody}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Exception in T2 {ex.ToString()}");
                        }
                    });

                    Task t3 = Task.Run(async () =>
                    {
                        try
                        {
                            EventPosition startingPosition = EventPosition.Earliest;
                            string partitionID = (await consumer.GetPartitionIdsAsync()).First();

                            using CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                            cancellationTokenSource.CancelAfter(TimeSpan.FromHours(1));

                            await foreach (PartitionEvent receivedEvent in consumer.ReadEventsFromPartitionAsync(partitionID, startingPosition, cancellationTokenSource.Token))
                            {
                                Console.WriteLine($"T3 Earliest ReceivedEvent : {receivedEvent.Partition.PartitionId} | {receivedEvent.Data.EventBody}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Exception in T3 {ex.ToString()}");
                        }
                    });

                    Task t4 = Task.Run(async () =>
                    {
                        try
                        {
                            EventPosition startingPosition = EventPosition.Latest;
                            string partitionID = (await consumer.GetPartitionIdsAsync()).First();

                            using CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                            cancellationTokenSource.CancelAfter(TimeSpan.FromHours(1));

                            await foreach (PartitionEvent receivedEvent in consumer.ReadEventsFromPartitionAsync(partitionID, startingPosition, cancellationTokenSource.Token))
                            {
                                Console.WriteLine($"T4 Latest ReceivedEvent : {receivedEvent.Partition.PartitionId} | {receivedEvent.Data.EventBody}");
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Exception in T4 {ex.ToString()}");
                        }
                    });

                    //t1.Wait();
                    t2.Wait();
                    t3.Wait();
                    t4.Wait();

                    //using CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                    //cancellationTokenSource.CancelAfter(TimeSpan.FromHours(1));

                    //await foreach (PartitionEvent receivedEvent in consumer.ReadEventsAsync(cancellationTokenSource.Token))
                    //{
                    //    Console.WriteLine($"ReceivedEvent : {receivedEvent.Partition.PartitionId} | {receivedEvent.Data.EventBody}");
                    //}
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exception {ex.ToString()}");
            }
        }
    }
}
