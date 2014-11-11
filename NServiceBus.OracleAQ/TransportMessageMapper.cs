namespace NServiceBus.Transports.OracleAQ
{
    using System;
    using System.IO;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Xml;
    using NServiceBus.Unicast;
    using Oracle.DataAccess.Client;
    using Oracle.DataAccess.Types;

    internal static class TransportMessageMapper
    {
        private static readonly Regex invalidCharcter = new Regex(@"[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD\u10000-\u10FFFF]", RegexOptions.Compiled);

        public static void SerializeToXml(TransportMessage transportMessage, SendOptions options, Stream stream)
        {
            using (var xmlWriter = XmlWriter.Create(stream, new XmlWriterSettings { CloseOutput = false, Encoding = new UTF8Encoding(false) }))
            {
                xmlWriter.WriteStartElement("TransportMessage");
                xmlWriter.WriteAttributeString("xmlns", "xsi", null, "http://www.w3.org/2001/XMLSchema-instance");
                xmlWriter.WriteAttributeString("xmlns", "xsd", null, "http://www.w3.org/2001/XMLSchema");
                {
                    xmlWriter.WriteStartElement("CorrelationId");
                    xmlWriter.WriteValue(transportMessage.CorrelationId);
                    xmlWriter.WriteEndElement();
                }

                {
                    xmlWriter.WriteStartElement("Recoverable");
                    xmlWriter.WriteValue(transportMessage.Recoverable);
                    xmlWriter.WriteEndElement();
                }

                {
                    xmlWriter.WriteStartElement("MessageIntent");
                    xmlWriter.WriteValue(transportMessage.MessageIntent.ToString());
                    xmlWriter.WriteEndElement();
                }

                {
                    if (options.ReplyToAddress != null)
                    {
                        xmlWriter.WriteStartElement("ReplyToAddress");
                        xmlWriter.WriteValue(options.ReplyToAddress.ToString());
                        xmlWriter.WriteEndElement();
                    }
                }

                {
                    var data = transportMessage.Body.EncodeToUTF8WithoutIdentifier();
                    var base64required = invalidCharcter.IsMatch(data);
                    xmlWriter.WriteStartElement("Body");
                    if (base64required)
                    {
                        xmlWriter.WriteAttributeString("isBase64", "true");
                        xmlWriter.WriteCData(Convert.ToBase64String(transportMessage.Body));
                    }
                    else
                    {
                        xmlWriter.WriteCData(data);
                    }

                    xmlWriter.WriteEndElement();
                }

                {
                    xmlWriter.WriteStartElement("Headers");
                    var headers = new SerializableDictionary(transportMessage.Headers);
                    headers.WriteXml(xmlWriter);
                    xmlWriter.WriteEndElement();
                }

                xmlWriter.WriteEndElement();
            }

            stream.Seek(0, SeekOrigin.Begin);
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

            var bodySection = bodyDoc.DocumentElement["Body"];
            byte[] bodyBytes = new byte[0];
            if (bodySection != null)
            {
                var bodySectionData = bodySection.FirstChild as XmlCDataSection;
                if (bodySection.Attributes["isBase64"] != null && bodySection.Attributes["isBase64"].Value == "true")
                {
                    bodyBytes = Convert.FromBase64String(bodySectionData.Data);
                }
                else
                {
                    bodyBytes = Encoding.UTF8.GetBytes(bodySectionData.Data);
                }
            }

            var headerDictionary = new SerializableDictionary();
            var headerSection = bodyDoc.DocumentElement["Headers"];
            if (headerSection != null)
            {
                headerDictionary.SetXml(headerSection);
            }

            var replyToAddressSection = bodyDoc.DocumentElement["ReplyToAddress"];
            if (replyToAddressSection != null && !string.IsNullOrWhiteSpace(replyToAddressSection.InnerText))
            {
                headerDictionary.Add(Headers.ReplyToAddress, replyToAddressSection.InnerText.Trim());
            }

            MessageIntentEnum messageIntent = default(MessageIntentEnum);
            var messageIntentSection = bodyDoc.DocumentElement["MessageIntent"];
            if (messageIntentSection != null)
            {
                messageIntent = (MessageIntentEnum)Enum.Parse(typeof(MessageIntentEnum), messageIntentSection.InnerText);
            }

            var transportMessage = new TransportMessage(new Guid(message.MessageId).ToString(), headerDictionary)
            {
                Body = bodyBytes,
                MessageIntent = messageIntent,
            };

            return transportMessage;
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
