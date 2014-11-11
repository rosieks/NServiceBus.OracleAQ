namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using NServiceBus.Satellites;
    using NServiceBus.Unicast;

    internal class PublishSatellite : ISatellite
    {
        private readonly IPublishMessages publishMessages;

        public PublishSatellite(IPublishMessages publishMessages)
        {
            this.publishMessages = publishMessages;
        }

        public bool Disabled
        {
            get;
            set;
        }

        public Address InputAddress
        {
            get;
            set;
        }

        public bool Handle(TransportMessage message)
        {
            var eventType = Type.GetType(message.Headers[Headers.EnclosedMessageTypes]);

            this.publishMessages.Publish(message, new PublishOptions(eventType));

            return true;
        }

        public void Start()
        {
        }

        public void Stop()
        {
        }
    }
}
