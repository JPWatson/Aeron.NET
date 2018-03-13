﻿using System;
using System.IO;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Agrona.Concurrent.Status;
using Adaptive.Archiver;
using Io.Aeron.Cluster.Codecs;

namespace Adaptive.Cluster.Service
{
    public sealed class ClusteredServiceContainer : IDisposable
    {
        public const int SYSTEM_COUNTER_TYPE_ID = 0;

        /// <summary>
        /// Type of snapshot for this service.
        /// </summary>
        public const long SNAPSHOT_TYPE_ID = 2;

        private readonly Context ctx;
        private readonly AgentRunner serviceAgentRunner;

        /// <summary>
        /// Launch the clustered service container and await a shutdown signal.
        /// </summary>
        /// <param name="args"> command line argument which is a list for properties files as URLs or filenames. </param>
        public static void Main(string[] args)
        {
            using (ClusteredServiceContainer container = Launch())
            {
                container.Ctx().ShutdownSignalBarrier().Await();

                Console.WriteLine("Shutdown ClusteredMediaDriver...");
            }
        }

        private ClusteredServiceContainer(Context ctx)
        {
            this.ctx = ctx;
            ctx.Conclude();

            ClusteredServiceAgent agent = new ClusteredServiceAgent(ctx);
            serviceAgentRunner = new AgentRunner(ctx.IdleStrategy(), ctx.ErrorHandler(), ctx.ErrorCounter(), agent);
        }

        private ClusteredServiceContainer Start()
        {
            AgentRunner.StartOnThread(serviceAgentRunner, ctx.ThreadFactory());
            return this;
        }

        /// <summary>
        /// Launch an ClusteredServiceContainer using a default configuration.
        /// </summary>
        /// <returns> a new instance of a ClusteredServiceContainer. </returns>
        public static ClusteredServiceContainer Launch()
        {
            return Launch(new Context());
        }

        /// <summary>
        /// Launch a ClusteredServiceContainer by providing a configuration context.
        /// </summary>
        /// <param name="ctx"> for the configuration parameters. </param>
        /// <returns> a new instance of a ClusteredServiceContainer. </returns>
        public static ClusteredServiceContainer Launch(Context ctx)
        {
            return (new ClusteredServiceContainer(ctx)).Start();
        }

        /// <summary>
        /// Get the <seealso cref="Context"/> that is used by this <seealso cref="ClusteredServiceContainer"/>.
        /// </summary>
        /// <returns> the <seealso cref="Context"/> that is used by this <seealso cref="ClusteredServiceContainer"/>. </returns>
        public Context Ctx()
        {
            return ctx;
        }

        public void Dispose()
        {
            serviceAgentRunner?.Dispose();
            ctx?.Dispose();
        }

        /// <summary>
        /// Configuration options for the consensus module and service container within a cluster.
        /// </summary>
        public class Configuration
        {
            /// <summary>
            /// Identity for a clustered service.
            /// </summary>
            public const string SERVICE_ID_PROP_NAME = "aeron.cluster.service.id";

            /// <summary>
            /// Identity for a clustered service. Default to 0.
            /// </summary>
            public const long SERVICE_ID_DEFAULT = 0;

            /// <summary>
            /// Channel for the clustered log.
            /// </summary>
            public const string LOG_CHANNEL_PROP_NAME = "aeron.cluster.log.channel";

            /// <summary>
            /// Channel for the clustered log. Default to localhost:9030.
            /// </summary>
            public const string LOG_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:9030";

            /// <summary>
            /// Stream id within a channel for the clustered log.
            /// </summary>
            public const string LOG_STREAM_ID_PROP_NAME = "aeron.cluster.log.stream.id";

            /// <summary>
            /// Stream id within a channel for the clustered log. Default to stream id of 3.
            /// </summary>
            public const int LOG_STREAM_ID_DEFAULT = 3;

            /// <summary>
            /// Channel to be used for log or snapshot replay on startup.
            /// </summary>
            public const string REPLAY_CHANNEL_PROP_NAME = "aeron.cluster.replay.channel";

            /// <summary>
            /// Channel to be used for log or snapshot replay on startup.
            /// </summary>
            public static readonly string REPLAY_CHANNEL_DEFAULT = Aeron.Aeron.Context.IPC_CHANNEL;

            /// <summary>
            /// Stream id within a channel for the clustered log or snapshot replay.
            /// </summary>
            public const string REPLAY_STREAM_ID_PROP_NAME = "aeron.cluster.replay.stream.id";

            /// <summary>
            /// Stream id for the log or snapshot replay within a channel.
            /// </summary>
            public const int REPLAY_STREAM_ID_DEFAULT = 4;

            /// <summary>
            /// Channel for sending messages to the Consensus Module.
            /// </summary>
            public const string CONSENSUS_MODULE_CHANNEL_PROP_NAME = "aeron.consensus.module.channel";

            /// <summary>
            /// Channel for for sending messages to the Consensus Module. This should be IPC.
            /// </summary>
            public static readonly string CONSENSUS_MODULE_CHANNEL_DEFAULT = Aeron.Aeron.Context.IPC_CHANNEL;

            /// <summary>
            /// Stream id within a channel for sending messages to the Consensus Module.
            /// </summary>
            public const string CONSENSUS_MODULE_STREAM_ID_PROP_NAME = "aeron.consensus.module.stream.id";

            /// <summary>
            /// Stream id within a channel for sending messages to the Consensus Module. Default to stream id of 5.
            /// </summary>
            public const int CONSENSUS_MODULE_STREAM_ID_DEFAULT = 5;

            /// <summary>
            /// Channel to be used for archiving snapshots.
            /// </summary>
            public const string SNAPSHOT_CHANNEL_PROP_NAME = "aeron.cluster.snapshot.channel";

            /// <summary>
            /// Channel to be used for archiving snapshots.
            /// </summary>
            public static readonly string SNAPSHOT_CHANNEL_DEFAULT = Aeron.Aeron.Context.IPC_CHANNEL;

            /// <summary>
            /// Stream id within a channel for archiving snapshots.
            /// </summary>
            public const string SNAPSHOT_STREAM_ID_PROP_NAME = "aeron.cluster.snapshot.stream.id";

            /// <summary>
            /// Stream id for the archived snapshots within a channel.
            /// </summary>
            public const int SNAPSHOT_STREAM_ID_DEFAULT = 7;

            /// <summary>
            /// Directory to use for the clustered service.
            /// </summary>
            public const string CLUSTERED_SERVICE_DIR_PROP_NAME = "aeron.clustered.service.dir";

            /// <summary>
            /// Directory to use for the cluster container.
            /// </summary>
            public const string CLUSTERED_SERVICE_DIR_DEFAULT = "clustered-service";

            /// <summary>
            /// The value <seealso cref="#SERVICE_ID_DEFAULT"/> or system property <seealso cref="#SERVICE_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#SERVICE_ID_DEFAULT"/> or system property <seealso cref="#SERVICE_ID_PROP_NAME"/> if set. </returns>
            public static long ServiceId()
            {
                return Config.GetLong(SERVICE_ID_PROP_NAME, SERVICE_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#LOG_CHANNEL_DEFAULT"/> or system property <seealso cref="#LOG_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#LOG_CHANNEL_DEFAULT"/> or system property <seealso cref="#LOG_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string LogChannel()
            {
                return Config.GetProperty(LOG_CHANNEL_PROP_NAME, LOG_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#LOG_STREAM_ID_DEFAULT"/> or system property <seealso cref="#LOG_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#LOG_STREAM_ID_DEFAULT"/> or system property <seealso cref="#LOG_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int LogStreamId()
            {
                return Config.GetInteger(LOG_STREAM_ID_PROP_NAME, LOG_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#REPLAY_CHANNEL_DEFAULT"/> or system property <seealso cref="#REPLAY_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#REPLAY_CHANNEL_DEFAULT"/> or system property <seealso cref="#REPLAY_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string ReplayChannel()
            {
                return Config.GetProperty(REPLAY_CHANNEL_PROP_NAME, REPLAY_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#REPLAY_STREAM_ID_DEFAULT"/> or system property <seealso cref="#REPLAY_STREAM_ID_PROP_NAME"/>
            /// if set.
            /// </summary>
            /// <returns> <seealso cref="#REPLAY_STREAM_ID_DEFAULT"/> or system property <seealso cref="#REPLAY_STREAM_ID_PROP_NAME"/>
            /// if set. </returns>
            public static int ReplayStreamId()
            {
                return Config.GetInteger(REPLAY_STREAM_ID_PROP_NAME, REPLAY_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#CONSENSUS_MODULE_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="#CONSENSUS_MODULE_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#CONSENSUS_MODULE_CHANNEL_DEFAULT"/> or system property
            /// <seealso cref="#CONSENSUS_MODULE_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string ConsensusModuleChannel()
            {
                return Config.GetProperty(CONSENSUS_MODULE_CHANNEL_PROP_NAME, CONSENSUS_MODULE_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#CONSENSUS_MODULE_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="#CONSENSUS_MODULE_STREAM_ID_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#CONSENSUS_MODULE_STREAM_ID_DEFAULT"/> or system property
            /// <seealso cref="#CONSENSUS_MODULE_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int ConsensusModuleStreamId()
            {
                return Config.GetInteger(CONSENSUS_MODULE_STREAM_ID_PROP_NAME, CONSENSUS_MODULE_STREAM_ID_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#SNAPSHOT_CHANNEL_DEFAULT"/> or system property <seealso cref="#SNAPSHOT_CHANNEL_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#SNAPSHOT_CHANNEL_DEFAULT"/> or system property <seealso cref="#SNAPSHOT_CHANNEL_PROP_NAME"/> if set. </returns>
            public static string SnapshotChannel()
            {
                return Config.GetProperty(SNAPSHOT_CHANNEL_PROP_NAME, SNAPSHOT_CHANNEL_DEFAULT);
            }

            /// <summary>
            /// The value <seealso cref="#SNAPSHOT_STREAM_ID_DEFAULT"/> or system property <seealso cref="#SNAPSHOT_STREAM_ID_PROP_NAME"/>
            /// if set.
            /// </summary>
            /// <returns> <seealso cref="#SNAPSHOT_STREAM_ID_DEFAULT"/> or system property <seealso cref="#SNAPSHOT_STREAM_ID_PROP_NAME"/> if set. </returns>
            public static int SnapshotStreamId()
            {
                return Config.GetInteger(SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT);
            }

            public const string DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";
            public const string CLUSTER_IDLE_STRATEGY_PROP_NAME = "aeron.cluster.idle.strategy";

            /// <summary>
            /// Create a supplier of <seealso cref="IdleStrategy"/>s that will use the system property.
            /// </summary>
            /// <param name="controllableStatus"> if a <seealso cref="org.agrona.concurrent.ControllableIdleStrategy"/> is required. </param>
            /// <returns> the new idle strategy </returns>
            public static Func<IIdleStrategy> IdleStrategySupplier(StatusIndicator controllableStatus)
            {
                return () =>
                {
                    var name = Config.GetProperty(CLUSTER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);
                    return IdleStrategyFactory.Create(name, controllableStatus);
                };
            }

            /// <summary>
            /// The value <seealso cref="#CLUSTERED_SERVICE_DIR_DEFAULT"/> or system property <seealso cref="#CLUSTERED_SERVICE_DIR_PROP_NAME"/> if set.
            /// </summary>
            /// <returns> <seealso cref="#CLUSTERED_SERVICE_DIR_DEFAULT"/> or system property <seealso cref="#CLUSTERED_SERVICE_DIR_PROP_NAME"/> if set. </returns>
            public static string ClusteredServiceDirName()
            {
                return Config.GetProperty(CLUSTERED_SERVICE_DIR_PROP_NAME, CLUSTERED_SERVICE_DIR_DEFAULT);
            }
        }

        public class Context : IDisposable
        {
            internal long serviceId = Configuration.ServiceId();
            internal string logChannel = Configuration.LogChannel();
            internal int logStreamId = Configuration.LogStreamId();
            internal string replayChannel = Configuration.ReplayChannel();
            internal int replayStreamId = Configuration.ReplayStreamId();
            internal string consensusModuleChannel = Configuration.ConsensusModuleChannel();
            internal int consensusModuleStreamId = Configuration.ConsensusModuleStreamId();
            internal string snapshotChannel = Configuration.SnapshotChannel();
            internal int snapshotStreamId = Configuration.SnapshotStreamId();
            internal bool deleteDirOnStart = false;

            internal IThreadFactory threadFactory;
            internal Func<IIdleStrategy> idleStrategySupplier;
            internal IEpochClock epochClock;
            internal ErrorHandler errorHandler;
            internal AtomicCounter errorCounter;
            internal CountedErrorHandler countedErrorHandler;
            internal AeronArchive.Context archiveContext;
            internal DirectoryInfo clusteredServiceDir;
            internal string aeronDirectoryName = Adaptive.Aeron.Aeron.Context.AERON_DIR_PROP_DEFAULT;
            internal Aeron.Aeron aeron;
            internal bool ownsAeronClient;

            internal IClusteredService clusteredService;
            internal RecordingLog recordingLog;
            internal ShutdownSignalBarrier shutdownSignalBarrier;
            internal Action terminationHook;

            public void Conclude()
            {
                if (null == threadFactory)
                {
                    threadFactory = new DefaultThreadFactory();
                }

                if (null == idleStrategySupplier)
                {
                    idleStrategySupplier = Configuration.IdleStrategySupplier(null);
                }

                if (null == epochClock)
                {
                    epochClock = new SystemEpochClock();
                }

                if (null == errorHandler)
                {
                    throw new InvalidOperationException("Error handler must be supplied");
                }

               
                if (null == aeron)
                {
                    var context = new Aeron.Aeron.Context()
                        .AeronDirectoryName(aeronDirectoryName)
                        .EpochClock(epochClock);

                    if (countedErrorHandler != null)
                    {
                        context.ErrorHandler(countedErrorHandler.OnError);
                    }

                    aeron = Adaptive.Aeron.Aeron.Connect(
                        context
                    );

                    if (null == errorCounter)
                    {
                        errorCounter = aeron.AddCounter(SYSTEM_COUNTER_TYPE_ID,
                            "Cluster errors - service " + serviceId);
                    }

                    ownsAeronClient = true;
                }

                if (null == errorCounter)
                {
                    throw new InvalidOperationException("Error counter must be supplied");
                }
                
                if (null == countedErrorHandler)
                {
                    countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
                    if (ownsAeronClient)
                    {
                        aeron.Ctx().ErrorHandler(countedErrorHandler.OnError);
                    }
                }

                if (null == archiveContext)
                {
                    archiveContext = (new AeronArchive.Context()).AeronClient(aeron).OwnsAeronClient(false)
                        .ControlRequestChannel(AeronArchive.Configuration.LocalControlChannel())
                        .ControlRequestStreamId(AeronArchive.Configuration.LocalControlStreamId()).Lock(new NoOpLock());
                }

                if (deleteDirOnStart)
                {
                    if (null != clusteredServiceDir)
                    {
                        IoUtil.Delete(clusteredServiceDir, true);
                    }
                    else
                    {
                        IoUtil.Delete(new DirectoryInfo(Configuration.ClusteredServiceDirName()), true);
                    }
                }

                if (null == clusteredServiceDir)
                {
                    clusteredServiceDir = new DirectoryInfo(Configuration.ClusteredServiceDirName());
                }

                if (!clusteredServiceDir.Exists)
                {
                    Directory.CreateDirectory(clusteredServiceDir.FullName);
                }

                if (null == recordingLog)
                {
                    recordingLog = new RecordingLog(clusteredServiceDir);
                }

                if (null == shutdownSignalBarrier)
                {
                    shutdownSignalBarrier = new ShutdownSignalBarrier();
                }

                if (null == terminationHook)
                {
                    terminationHook = () => shutdownSignalBarrier.Signal();
                }
            }

            /// <summary>
            /// Set the id for this clustered service.
            /// </summary>
            /// <param name="serviceId"> for this clustered service. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME </seealso>
            public Context ServiceId(long serviceId)
            {
                this.serviceId = serviceId;
                return this;
            }

            /// <summary>
            /// Get the id for this clustered service.
            /// </summary>
            /// <returns> the id for this clustered service. </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#SERVICE_ID_PROP_NAME </seealso>
            public long ServiceId()
            {
                return serviceId;
            }

            /// <summary>
            /// Set the channel parameter for the cluster log channel.
            /// </summary>
            /// <param name="channel"> parameter for the cluster log channel. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#LOG_CHANNEL_PROP_NAME </seealso>
            public Context LogChannel(string channel)
            {
                logChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for the cluster log channel.
            /// </summary>
            /// <returns> the channel parameter for the cluster channel. </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#LOG_CHANNEL_PROP_NAME </seealso>
            public string LogChannel()
            {
                return logChannel;
            }

            /// <summary>
            /// Set the stream id for the cluster log channel.
            /// </summary>
            /// <param name="streamId"> for the cluster log channel. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#LOG_STREAM_ID_PROP_NAME </seealso>
            public Context LogStreamId(int streamId)
            {
                logStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for the cluster log channel.
            /// </summary>
            /// <returns> the stream id for the cluster log channel. </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#LOG_STREAM_ID_PROP_NAME </seealso>
            public int LogStreamId()
            {
                return logStreamId;
            }

            /// <summary>
            /// Set the channel parameter for the cluster log and snapshot replay channel.
            /// </summary>
            /// <param name="channel"> parameter for the cluster log replay channel. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#REPLAY_CHANNEL_PROP_NAME </seealso>
            public Context ReplayChannel(string channel)
            {
                replayChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for the cluster log and snapshot replay channel.
            /// </summary>
            /// <returns> the channel parameter for the cluster replay channel. </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#REPLAY_CHANNEL_PROP_NAME </seealso>
            public string ReplayChannel()
            {
                return replayChannel;
            }

            /// <summary>
            /// Set the stream id for the cluster log and snapshot replay channel.
            /// </summary>
            /// <param name="streamId"> for the cluster log replay channel. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#REPLAY_STREAM_ID_PROP_NAME </seealso>
            public Context ReplayStreamId(int streamId)
            {
                replayStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for the cluster log and snapshot replay channel.
            /// </summary>
            /// <returns> the stream id for the cluster log replay channel. </returns>
            /// <seealso cref= ClusteredServiceContainer.Configuration#REPLAY_STREAM_ID_PROP_NAME </seealso>
            public int ReplayStreamId()
            {
                return replayStreamId;
            }

            /// <summary>
            /// Set the channel parameter for sending messages to the Consensus Module.
            /// </summary>
            /// <param name="channel"> parameter for sending messages to the Consensus Module. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref= Configuration#CONSENSUS_MODULE_CHANNEL_PROP_NAME </seealso>
            public Context ConsensusModuleChannel(string channel)
            {
                consensusModuleChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for sending messages to the Consensus Module.
            /// </summary>
            /// <returns> the channel parameter for sending messages to the Consensus Module. </returns>
            /// <seealso cref= Configuration#CONSENSUS_MODULE_CHANNEL_PROP_NAME </seealso>
            public string ConsensusModuleChannel()
            {
                return consensusModuleChannel;
            }

            /// <summary>
            /// Set the stream id for sending messages to the Consensus Module.
            /// </summary>
            /// <param name="streamId"> for sending messages to the Consensus Module. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref= Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME </seealso>
            public Context ConsensusModuleStreamId(int streamId)
            {
                consensusModuleStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for sending messages to the Consensus Module.
            /// </summary>
            /// <returns> the stream id for sending messages to the Consensus Module. </returns>
            /// <seealso cref= Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME </seealso>
            public int ConsensusModuleStreamId()
            {
                return consensusModuleStreamId;
            }

            /// <summary>
            /// Set the channel parameter for snapshot recordings.
            /// </summary>
            /// <param name="channel"> parameter for snapshot recordings </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref= Configuration#SNAPSHOT_CHANNEL_PROP_NAME </seealso>
            public Context SnapshotChannel(string channel)
            {
                snapshotChannel = channel;
                return this;
            }

            /// <summary>
            /// Get the channel parameter for snapshot recordings.
            /// </summary>
            /// <returns> the channel parameter for snapshot recordings. </returns>
            /// <seealso cref= Configuration#SNAPSHOT_CHANNEL_PROP_NAME </seealso>
            public string SnapshotChannel()
            {
                return snapshotChannel;
            }

            /// <summary>
            /// Set the stream id for snapshot recordings.
            /// </summary>
            /// <param name="streamId"> for snapshot recordings. </param>
            /// <returns> this for a fluent API </returns>
            /// <seealso cref= Configuration#SNAPSHOT_STREAM_ID_PROP_NAME </seealso>
            public Context SnapshotStreamId(int streamId)
            {
                snapshotStreamId = streamId;
                return this;
            }

            /// <summary>
            /// Get the stream id for snapshot recordings.
            /// </summary>
            /// <returns> the stream id for snapshot recordings. </returns>
            /// <seealso cref= Configuration#SNAPSHOT_STREAM_ID_PROP_NAME </seealso>
            public int SnapshotStreamId()
            {
                return snapshotStreamId;
            }

            /// <summary>
            /// Get the thread factory used for creating threads.
            /// </summary>
            /// <returns> thread factory used for creating threads. </returns>
            public IThreadFactory ThreadFactory()
            {
                return threadFactory;
            }

            /// <summary>
            /// Set the thread factory used for creating threads.
            /// </summary>
            /// <param name="threadFactory"> used for creating threads </param>
            /// <returns> this for a fluent API. </returns>
            public Context ThreadFactory(IThreadFactory threadFactory)
            {
                this.threadFactory = threadFactory;
                return this;
            }

            /// <summary>
            /// Provides an <seealso cref="IdleStrategy"/> supplier for the thread responsible for publication/subscription backoff.
            /// </summary>
            /// <param name="idleStrategySupplier"> supplier of thread idle strategy for publication/subscription backoff. </param>
            /// <returns> this for a fluent API. </returns>
            public Context IdleStrategySupplier(Func<IIdleStrategy> idleStrategySupplier)
            {
                this.idleStrategySupplier = idleStrategySupplier;
                return this;
            }

            /// <summary>
            /// Get a new <seealso cref="IdleStrategy"/> based on configured supplier.
            /// </summary>
            /// <returns> a new <seealso cref="IdleStrategy"/> based on configured supplier. </returns>
            public IIdleStrategy IdleStrategy()
            {
                return idleStrategySupplier();
            }

            /// <summary>
            /// Set the <seealso cref="EpochClock"/> to be used for tracking wall clock time when interacting with the archive.
            /// </summary>
            /// <param name="clock"> <seealso cref="EpochClock"/> to be used for tracking wall clock time when interacting with the archive. </param>
            /// <returns> this for a fluent API. </returns>
            public Context EpochClock(IEpochClock clock)
            {
                this.epochClock = clock;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="EpochClock"/> to used for tracking wall clock time within the archive.
            /// </summary>
            /// <returns> the <seealso cref="EpochClock"/> to used for tracking wall clock time within the archive. </returns>
            public IEpochClock EpochClock()
            {
                return epochClock;
            }

            /// <summary>
            /// Get the <seealso cref="Agrona.ErrorHandler"/> to be used by the Archive.
            /// </summary>
            /// <returns> the <seealso cref="Agrona.ErrorHandler"/> to be used by the Archive. </returns>
            public ErrorHandler ErrorHandler()
            {
                return errorHandler;
            }

            /// <summary>
            /// Set the <seealso cref="Agrona.ErrorHandler"/> to be used by the Archive.
            /// </summary>
            /// <param name="errorHandler"> the error handler to be used by the Archive. </param>
            /// <returns> this for a fluent API </returns>
            public Context ErrorHandler(ErrorHandler errorHandler)
            {
                this.errorHandler = errorHandler;
                return this;
            }

            /// <summary>
            /// Get the error counter that will record the number of errors the archive has observed.
            /// </summary>
            /// <returns> the error counter that will record the number of errors the archive has observed. </returns>
            public AtomicCounter ErrorCounter()
            {
                return errorCounter;
            }

            /// <summary>
            /// Set the error counter that will record the number of errors the cluster node has observed.
            /// </summary>
            /// <param name="errorCounter"> the error counter that will record the number of errors the cluster node has observed. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ErrorCounter(AtomicCounter errorCounter)
            {
                this.errorCounter = errorCounter;
                return this;
            }

            /// <summary>
            /// Non-default for context.
            /// </summary>
            /// <param name="countedErrorHandler"> to override the default. </param>
            /// <returns> this for a fluent API. </returns>
            public Context CountedErrorHandler(CountedErrorHandler countedErrorHandler)
            {
                this.countedErrorHandler = countedErrorHandler;
                return this;
            }

            /// <summary>
            /// The <seealso cref="#errorHandler()"/> that will increment <seealso cref="#errorCounter()"/> by default.
            /// </summary>
            /// <returns> <seealso cref="#errorHandler()"/> that will increment <seealso cref="#errorCounter()"/> by default. </returns>
            public CountedErrorHandler CountedErrorHandler()
            {
                return countedErrorHandler;
            }

            /// <summary>
            /// Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
            /// </summary>
            /// <param name="aeronDirectoryName"> the top level Aeron directory. </param>
            /// <returns> this for a fluent API. </returns>
            public Context AeronDirectoryName(string aeronDirectoryName)
            {
                this.aeronDirectoryName = aeronDirectoryName;
                return this;
            }

            /// <summary>
            /// Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
            /// </summary>
            /// <returns> The top level Aeron directory. </returns>
            public string AeronDirectoryName()
            {
                return aeronDirectoryName;
            }

            /// <summary>
            /// An <seealso cref="Adaptive.Aeron.Aeron"/> client for the container.
            /// </summary>
            /// <returns> <seealso cref="Adaptive.Aeron.Aeron"/> client for the container </returns>
            public Aeron.Aeron Aeron()
            {
                return aeron;
            }

            /// <summary>
            /// Provide an <seealso cref="Adaptive.Aeron.Aeron"/> client for the container
            /// <para>
            /// If not provided then one will be created.
            /// 
            /// </para>
            /// </summary>
            /// <param name="aeron"> client for the container </param>
            /// <returns> this for a fluent API. </returns>
            public Context Aeron(Aeron.Aeron aeron)
            {
                this.aeron = aeron;
                return this;
            }

            /// <summary>
            /// Does this context own the <seealso cref="#aeron()"/> client and this takes responsibility for closing it?
            /// </summary>
            /// <param name="ownsAeronClient"> does this context own the <seealso cref="#aeron()"/> client. </param>
            /// <returns> this for a fluent API. </returns>
            public Context OwnsAeronClient(bool ownsAeronClient)
            {
                this.ownsAeronClient = ownsAeronClient;
                return this;
            }

            /// <summary>
            /// Does this context own the <seealso cref="#aeron()"/> client and this takes responsibility for closing it?
            /// </summary>
            /// <returns> does this context own the <seealso cref="#aeron()"/> client and this takes responsibility for closing it? </returns>
            public bool OwnsAeronClient()
            {
                return ownsAeronClient;
            }

            /// <summary>
            /// The service this container holds.
            /// </summary>
            /// <returns> service this container holds. </returns>
            public IClusteredService ClusteredService()
            {
                return clusteredService;
            }

            /// <summary>
            /// Set the service this container is to hold.
            /// </summary>
            /// <param name="clusteredService"> this container is to hold. </param>
            /// <returns> this for fluent API. </returns>
            public Context ClusteredService(IClusteredService clusteredService)
            {
                this.clusteredService = clusteredService;
                return this;
            }

            /// <summary>
            /// Set the <seealso cref="AeronArchive.Context"/> that should be used for communicating with the local Archive.
            /// </summary>
            /// <param name="archiveContext"> that should be used for communicating with the local Archive. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ArchiveContext(AeronArchive.Context archiveContext)
            {
                this.archiveContext = archiveContext;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="AeronArchive.Context"/> that should be used for communicating with the local Archive.
            /// </summary>
            /// <returns> the <seealso cref="AeronArchive.Context"/> that should be used for communicating with the local Archive. </returns>
            public AeronArchive.Context ArchiveContext()
            {
                return archiveContext;
            }

            /// <summary>
            /// Should the container attempt to immediately delete <seealso cref="#clusteredServiceDir()"/> on startup.
            /// </summary>
            /// <param name="deleteDirOnStart"> Attempt deletion. </param>
            /// <returns> this for a fluent API. </returns>
            public Context DeleteDirOnStart(bool deleteDirOnStart)
            {
                this.deleteDirOnStart = deleteDirOnStart;
                return this;
            }

            /// <summary>
            /// Will the container attempt to immediately delete <seealso cref="#clusteredServiceDir()"/> on startup.
            /// </summary>
            /// <returns> true when directory will be deleted, otherwise false. </returns>
            public bool DeleteDirOnStart()
            {
                return deleteDirOnStart;
            }

            /// <summary>
            /// Set the directory to use for the clustered service container.
            /// </summary>
            /// <param name="dir"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            /// <seealso cref= Configuration#CLUSTERED_SERVICE_DIR_PROP_NAME </seealso>
            public Context ClusteredServiceDir(DirectoryInfo dir)
            {
                this.clusteredServiceDir = dir;
                return this;
            }

            /// <summary>
            /// The directory used for the clustered service container.
            /// </summary>
            /// <returns> directory for the cluster container. </returns>
            /// <seealso cref= Configuration#CLUSTERED_SERVICE_DIR_PROP_NAME </seealso>
            public DirectoryInfo ClusteredServiceDir()
            {
                return clusteredServiceDir;
            }

            /// <summary>
            /// Set the <seealso cref="Service.RecordingLog"/> for the  log terms and snapshots.
            /// </summary>
            /// <param name="recordingLog"> to use. </param>
            /// <returns> this for a fluent API. </returns>
            public Context RecordingLog(RecordingLog recordingLog)
            {
                this.recordingLog = recordingLog;
                return this;
            }

            /// <summary>
            /// The <seealso cref="Service.RecordingLog"/> for the  log terms and snapshots.
            /// </summary>
            /// <returns> <seealso cref="Service.RecordingLog"/> for the  log terms and snapshots. </returns>
            public RecordingLog RecordingLog()
            {
                return recordingLog;
            }

            /// <summary>
            /// Set the <seealso cref="ShutdownSignalBarrier"/> that can be used to shutdown a clustered service.
            /// </summary>
            /// <param name="barrier"> that can be used to shutdown a clustered service. </param>
            /// <returns> this for a fluent API. </returns>
            public Context ShutdownSignalBarrier(ShutdownSignalBarrier barrier)
            {
                shutdownSignalBarrier = barrier;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="ShutdownSignalBarrier"/> that can be used to shutdown a clustered service.
            /// </summary>
            /// <returns> the <seealso cref="ShutdownSignalBarrier"/> that can be used to shutdown a clustered service. </returns>
            public ShutdownSignalBarrier ShutdownSignalBarrier()
            {
                return shutdownSignalBarrier;
            }

            /// <summary>
            /// Set the <seealso cref="Action"/> that is called when processing a
            /// <seealso cref="ServiceAction.SHUTDOWN"/> or <seealso cref="ServiceAction.ABORT"/>
            /// </summary>
            /// <param name="terminationHook"> that can be used to terminate a service container. </param>
            /// <returns> this for a fluent API. </returns>
            public Context TerminationHook(Action terminationHook)
            {
                this.terminationHook = terminationHook;
                return this;
            }

            /// <summary>
            /// Get the <seealso cref="Action"/> that is called when processing a
            /// <seealso cref="ServiceAction.SHUTDOWN"/> or <seealso cref="ServiceAction.ABORT"/>
            /// <para>
            /// The default action is to call signal on the <seealso cref="#shutdownSignalBarrier()"/>.
            /// 
            /// </para>
            /// </summary>
            /// <returns> the <seealso cref="Action"/> that can be used to terminate a service container. </returns>
            public Action TerminationHook()
            {
                return terminationHook;
            }

            /// <summary>
            /// Delete the cluster container directory.
            /// </summary>
            public void DeleteDirectory()
            {
                if (null != clusteredServiceDir)
                {
                    IoUtil.Delete(clusteredServiceDir, false);
                }
            }

            /// <summary>
            /// Close the context and free applicable resources.
            /// <para>
            /// If <seealso cref="OwnsAeronClient()"/> is true then the <seealso cref="Aeron()"/> client will be closed.
            /// </para>
            /// </summary>
            public void Dispose()
            {
                if (ownsAeronClient)
                {
                    aeron?.Dispose();
                }
            }
        }
    }
}