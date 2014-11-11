namespace NServiceBus.Transports.OracleAQ
{
    using System.Collections.Generic;
    using System.Text;
    using System.Xml;
    using System.Xml.Serialization;

    [XmlRoot("dictionary")]
    internal class SerializableDictionary
        : Dictionary<string, string>, IXmlSerializable
    {
        public SerializableDictionary()
        {
        }

        public SerializableDictionary(IDictionary<string, string> dictionary)
            : base(dictionary)
        {
        }

        #region IXmlSerializable Members
        public System.Xml.Schema.XmlSchema GetSchema()
        {
            return null;
        }

        public void ReadXml(System.Xml.XmlReader reader)
        {
            bool wasEmpty = reader.IsEmptyElement;
            reader.Read();

            if (wasEmpty)
            {
                return;
            }

            while (reader.NodeType != System.Xml.XmlNodeType.EndElement && reader.NodeType != XmlNodeType.None)
            {
                reader.ReadStartElement("item");

                reader.ReadStartElement("key");
                reader.ReadStartElement("string");
                string key = reader.ReadContentAsString();
                reader.ReadEndElement();
                reader.ReadEndElement();

                string value = null;
                reader.ReadStartElement("value");
                if (!reader.IsEmptyElement)
                {
                    reader.ReadStartElement("string");
                    value = reader.ReadContentAsString();
                    reader.ReadEndElement();
                    reader.ReadEndElement();
                    reader.ReadEndElement();
                }

                this.Add(key, value);
                reader.MoveToContent();
            }
        }

        public void WriteXml(System.Xml.XmlWriter writer)
        {
            foreach (string key in this.Keys)
            {
                writer.WriteStartElement("item");

                writer.WriteStartElement("key");
                writer.WriteStartElement("string");
                writer.WriteValue(key);
                writer.WriteEndElement();
                writer.WriteEndElement();

                writer.WriteStartElement("value");
                writer.WriteStartElement("string");
                writer.WriteValue(this[key]);
                writer.WriteEndElement();
                writer.WriteEndElement();

                writer.WriteEndElement();
            }
        }
        #endregion

        public void SetXml(XmlNode node)
        {
            using (var nodeReader = new XmlNodeReader(node))
            {
                using (var reader = XmlReader.Create(nodeReader, new XmlReaderSettings { IgnoreWhitespace = true }))
                {
                    reader.Read();

                    this.ReadXml(reader);
                }
            }
        }

        public string GetXml()
        {
            StringBuilder sb = new StringBuilder();
            using (var writer = XmlWriter.Create(sb, new XmlWriterSettings { ConformanceLevel = ConformanceLevel.Fragment }))
            {
                this.WriteXml(writer);
            }

            return sb.ToString();
        }
    }
}
