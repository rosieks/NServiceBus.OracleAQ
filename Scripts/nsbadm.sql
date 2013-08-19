--------------------------------------------------------
--  DDL for Package NSBADM
--------------------------------------------------------

CREATE OR REPLACE PACKAGE NSBADM AS

  PROCEDURE RETRY (queue VARCHAR2, msgid RAW);

  PROCEDURE CREATE_QUEUE (queue VARCHAR2, queue_table VARCHAR2);

END NSBADM;

/
--------------------------------------------------------
--  DDL for Package Body NSBADM
--------------------------------------------------------

CREATE OR REPLACE PACKAGE BODY NSBADM AS

  PROCEDURE RETRY (queue VARCHAR2, msgid RAW) AS
    options DBMS_AQ.DEQUEUE_OPTIONS_T;
    enqueue_options DBMS_AQ.ENQUEUE_OPTIONS_T;
    properties DBMS_AQ.MESSAGE_PROPERTIES_T;
    payload XMLType;
    msgid_out RAW(16);
    failedQ VARCHAR2(30);
  BEGIN
    options.msgid := msgid;

    DBMS_AQ.DEQUEUE(
      queue_name => queue,
      dequeue_options => options,
      message_properties => properties,
      payload => payload,
      msgid => msgid_out);
    
    SELECT EXTRACTVALUE(payload, '//Headers/item[key/string="NServiceBus.FailedQ"]/value/string')
    INTO failedQ
    FROM dual;
    
    dbms_aq.enqueue(
      queue_name => failedQ,
      enqueue_options => enqueue_options,
      message_properties => properties,
      payload => payload,
      msgid => msgid_out);
    
  END RETRY;

  PROCEDURE CREATE_QUEUE (queue VARCHAR2, queue_table VARCHAR2) AS
    cnt NUMBER; 
  BEGIN

    SELECT count(*) INTO cnt FROM all_tables WHERE table_name = queue_table;
    IF cnt = 0 THEN
      dbms_aqadm.create_queue_table(queue_table, 'SYS.XMLType');
    END IF;
    
    SELECT count(*) INTO cnt FROM dba_queues WHERE name = :queue;
    IF cnt = 0 THEN
        dbms_aqadm.create_queue(
            queue_name => queue,
            queue_table => queue_table,
            max_retries => 999);
    END IF;
    DBMS_AQADM.START_QUEUE(queue);

  END CREATE_QUEUE;

END NSBADM;

/
