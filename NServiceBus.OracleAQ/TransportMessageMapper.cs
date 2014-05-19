namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.IO;
    using System.Text;
    using System.Xml;
    using System.Xml.Serialization;
    using Oracle.DataAccess.Client;
    using Oracle.DataAccess.Types;

    internal static class TransportMessageMapper
    {
        private static readonly XmlSerializer TransportMessageSerializer = CreateSerializer();

        public static void SerializeToXml(TransportMessage transportMessage, Stream stream)
        {
            var doc = new XmlDocument();

            using (var tempstream = new MemoryStream())
            {
                TransportMessageSerializer.Serialize(tempstream, transportMessage);
                tempstream.Position = 0;

                doc.Load(tempstream);
            }

            var data = transportMessage.Body.EncodeToUTF8WithoutIdentifier();

            var bodyElement = doc.CreateElement("Body");
            bodyElement.AppendChild(doc.CreateCDataSection(data));
            doc.DocumentElement.AppendChild(bodyElement);

            var headers = new SerializableDictionary<string, string>(transportMessage.Headers);

            var headerElement = doc.CreateElement("Headers");
            headerElement.InnerXml = headers.GetXml();
            doc.DocumentElement.AppendChild(headerElement);

            if (transportMessage.ReplyToAddress != null)
            {
                var replyToAddressElement = doc.CreateElement("ReplyToAddress");
                replyToAddressElement.InnerText = transportMessage.ReplyToAddress.ToString();
                doc.DocumentElement.AppendChild(replyToAddressElement);
            }

            doc.Save(stream);
            stream.Position = 0;
        }

        public static TransportMessage DeserializeFromXml(OracleAQMessage message)
        {
            if (message == null)
            {
                return null;
            }

            XmlDocument bodyDoc;
            using (OracleXmlType type = (OracleXmlType)message.Payload)
            {
                bodyDoc = type.GetXmlDocument();
            }

            var bodySection = bodyDoc.DocumentElement.SelectSingleNode("Body").FirstChild as XmlCDataSection;

            var headerDictionary = new SerializableDictionary<string, string>();
            var headerSection = bodyDoc.DocumentElement.SelectSingleNode("Headers");
            if (headerSection != null)
            {
                headerDictionary.SetXml(headerSection.InnerXml);
            }

            Address replyToAddress = null;
            var replyToAddressSection = bodyDoc.DocumentElement.SelectSingleNode("ReplyToAddress");
            if (replyToAddressSection != null && !string.IsNullOrWhiteSpace(replyToAddressSection.InnerText))
            {
                replyToAddress = Address.Parse(replyToAddressSection.InnerText.Trim());
            }

            MessageIntentEnum messageIntent = default(MessageIntentEnum);
            var messageIntentSection = bodyDoc.DocumentElement.SelectSingleNode("MessageIntent");
            if (messageIntentSection != null)
            {
                messageIntent = (MessageIntentEnum)Enum.Parse(typeof(MessageIntentEnum), messageIntentSection.InnerText);
            }

            var transportMessage = new TransportMessage(new Guid(message.MessageId).ToString(), headerDictionary)
            {
                Body = bodySection != null ? Encoding.UTF8.GetBytes(bodySection.Data) : new byte[0],
                ReplyToAddress = replyToAddress,
                MessageIntent = messageIntent,
            };

            return transportMessage;
        }

        private static XmlSerializer CreateSerializer()
        {
            var overrides = new XmlAttributeOverrides();
            var attrs = new XmlAttributes { XmlIgnore = true };

            overrides.Add(typeof(TransportMessage), "ReplyToAddress", attrs);
            overrides.Add(typeof(TransportMessage), "Headers", attrs);
            overrides.Add(typeof(TransportMessage), "Body", attrs);
            return new XmlSerializer(typeof(TransportMessage), overrides);
        }

        private static string EncodeToUTF8WithoutIdentifier(this byte[] bytes)
        {
            if (bytes != null)
            {
                if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
                {
                    return Encoding.UTF8.GetString(bytes, 3, bytes.Length - 3);
                }
                else
                {
                    return Encoding.UTF8.GetString(bytes);
                }
            }
            else
            {
                return string.Empty;
            }
        }
    }
}
