--------------------------------------------------------
--  DDL for Package NSB
--------------------------------------------------------

CREATE OR REPLACE PACKAGE NSB AS 

  TYPE HEADERS IS TABLE OF VARCHAR2(4000) INDEX BY VARCHAR2(50);

  PROCEDURE SEND (endpoint VARCHAR2, namespace VARCHAR2, message VARCHAR2, data VARCHAR2, headers NSB.HEADERS);

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
    data VARCHAR2,
    headers NSB.HEADERS) 
  AS
    options SYS.DBMS_AQ.ENQUEUE_OPTIONS_T;
    properties SYS.DBMS_AQ.MESSAGE_PROPERTIES_T;
    msgid RAW(16);
    transportMessage VARCHAR2(4000);
    transportMessageXml SYS.XMLType;
    headersXml VARCHAR2(4000) := '';
    headerKey VARCHAR2(50);
  BEGIN
  
    headerKey := headers.first;
    WHILE headerKey IS NOT NULL LOOP
      headersXml := headersXml || '<item><key><string>' || headerKey || '</string></key>'
                               || '<value><string>' || headers(headerKey) || '</string></value></item>';
      headerKey := headers.next(headerKey);
    END LOOP;
    
    transportMessage := '<?xml version="1.0"?><TransportMessage><Body><![CDATA[<Messages xmlns="http://tempuri.net/'
    || namespace || '"><'
    || message || '>'
    || data
    || '</' || message || '></Messages>]]></Body>'
    || '<Headers>' || headersXml || '</Headers>'
    || '</TransportMessage>';
    
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
