--------------------------------------------------------
--  DDL for Package NSB
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE NSB AS 

  PROCEDURE SEND (endpoint VARCHAR2, namespace VARCHAR2, message VARCHAR2, data VARCHAR2);

END NSB;

/
--------------------------------------------------------
--  DDL for Package Body NSB
--------------------------------------------------------

  CREATE OR REPLACE PACKAGE BODY NSB AS

  PROCEDURE SEND (
    endpoint VARCHAR2, 
    namespace VARCHAR2, 
    message VARCHAR2, 
    data VARCHAR2) 
  AS
    options SYS.DBMS_AQ.ENQUEUE_OPTIONS_T;
    properties SYS.DBMS_AQ.MESSAGE_PROPERTIES_T;
    msgid RAW(16);
    transportMessage VARCHAR2(4000);
    transportMessageXml SYS.XMLType;
  BEGIN
    
    transportMessage := '<?xml version="1.0"?><TransportMessage><Body><![CDATA[<Messages xmlns="http://tempuri.net/'
    || namespace || '"><'
    || message || '>'
    || data
    || '</' || message || '></Messages>]]></Body></TransportMessage>';
    
    transportMessageXml := SYS.XMLType.createXML(transportMessage);
    options.visibility := SYS.DBMS_AQ.ON_COMMIT;
    
    SYS.DBMS_AQ.ENQUEUE(
      queue_name => endpoint,
      enqueue_options => options,
      message_properties => properties,
      payload => transportMessageXml,
      msgid => msgid);
    
  END SEND;

END NSB;

/
