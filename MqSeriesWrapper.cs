namespace MqSeriesWrapper
{
    using System;
    using System.Collections;

    public class MqManager
    {
        IBM.WMQ.MQQueueManager _qQueueManager;
        SortedList _existingQueues;

        public MqManager()
        {
            _existingQueues = new _existingQueues();
        }

        public void Connect(string queueManagerName)
        {
            try
            {
                _mqQueueManager = new IBM.WMQ.MQQueueManager(queueManagerName);
            }
            catch(Exception ex)
            {
                throw new Exception(string.Format("Connect(): {0}, {1}", queueManagerName, ex.ToString());
            }
        }

        public void Disconnect()
        {
            _mqQueueManager.Disconnect();
        }

        public bool OpenQueueForRead(string queueName)
        {
            if (_existingQueues.Contains(queueName))
                return true;

            bool result = true;
            try
            {
                int queueOpenOptions    = IBM.WMQ.MQC.MQOO_INPUT_AS_Q_DEF | IBM.WMQ.MQC.MQOO_INQUIRE;
                IBM.WMQ.MQQueue mqQueue = _mqQueueManager.AccessQueue(queueName, queueOpenOptions);

                _existingQueues.Add(queueName, mqQueue);
            }
            catch(IBM.WMQ.MQException mqException)
            {
                if(IBM.WMQ.MQC.MQRC_UNKNOWN_OBJECT_NAME == mqException.ReasonCode) // there is no such queue
                {
                    // ... log or throw an exception of some type
                }
                else
                {
                    string errorMessage = String.Format // this line here is make more evident what is important to know about the error
                    (
                        "OpenQueueForRead(): reasonCode={0}, details={1}",
                        mqException.ReasonCode,
                        mqException.ToString()
                    );

                    throw new Exception( errorMessage );
                }

                result = false;
            }
            catch(Exception ex)
            {
                _existingQueues.Remove(queueName);

                result = false;
            }

            return result;
        }

        public bool OpenQueueForWrite(string queueName)
        {
            if(_existingQueues.Contains(queueName))
                return true;

            bool result = true;
            try
            {
                int queueOpenOptions    = IBM.WMQ.MQC.MQOO_OUTPUT;
                IBM.WMQ.MQQueue mqQueue = _mqQueueManager.AccessQueue(queueName, queueOpenOptions);

                _existingQueues.Add(queueName, mqQueue);
            }
            catch(IBM.WMQ.MQException mqException)
            {
                if( IBM.WMQ.MQC.MQRC_UNKNOWN_OBJECT_NAME == mqException.ReasonCode ) // there is no such queue
                {
                    // ... log or throw an exception of some type
                }
                else
                {
                    string errorMessage = String.Format // this line here is make more evident what is important to know about the error
                    (
                        "OpenQueueForWrite(): reasonCode={0}, details={1}",
                        mqException.ReasonCode,
                        mqException.ToString()
                    );

                    throw new Exception( errorMessage );
                }

                result = false;
            }
            catch( Exception exception )
            {
                _existingQueues.Remove( queueName );

                result = false;
            }

            return result;
        }

        public void CloseQueue(string queueName)
        {
            if(!_existingQueues.ContainsKey(queueName))
                return;

            IBM.WMQ.MQQueue	queue = (IBM.WMQ.MQQueue) _existingQueues[ queueName ];
            queue.Close();

            _existingQueues.Remove(queueName);
        }

        public void GetMessageWithSyncPoint(ref string queuName, ref IBM.WMQ.MQMessage mqMessage)
        {
            const int c_timeOut = 1000;
            getMessageWithSyncPoint( ref queuName, ref mqMessage, c_timeOut );
        }

        public void GetMessageWithSyncPoint( ref string queuName, ref byte[] messageContent )
        {
            IBM.WMQ.MQMessage mqMessage = null;
            getMessageWithSyncPoint( ref queuName, ref mqMessage );

            messageContent = mqMessage.ReadBytes( mqMessage.DataLength );
        }

        public void GetMessageWithSyncPoint( ref string queuName, ref string messageContent )
        {
            IBM.WMQ.MQMessage mqMessage = null;
            getMessageWithSyncPoint( ref queuName, ref mqMessage );

            byte[] messageContentAsByteArray = mqMessage.ReadBytes( mqMessage.DataLength );
            messageContent = Utils.byteArrayToString( ref messageContentAsByteArray );
        }


        public void GetMessageWithSyncPoint( ref string queuName, ref IBM.WMQ.MQMessage mqMessage, int timeOut )
        {
            if( false == _existingQueues.ContainsKey( queuName ) )
                return;

            try
            {
                IBM.WMQ.MQGetMessageOptions mqGetMessageOptions         = new IBM.WMQ.MQGetMessageOptions();
                mqGetMessageOptions.Options|= IBM.WMQ.MQC.MQGMO_SYNCPOINT;
                mqGetMessageOptions.Options|= IBM.WMQ.MQC.MQGMO_WAIT;
                mqGetMessageOptions.WaitInterval = timeOut;

                mqMessage = new IBM.WMQ.MQMessage();
                IBM.WMQ.MQQueue	queue = (IBM.WMQ.MQQueue) _existingQueues[ queuName ];
                queue.Get( mqMessage, mqGetMessageOptions );
            }
            catch( IBM.WMQ.MQException mqException ) // some nasty error 1
            {
                string errorMessage = mqException.ToString();

                if( 2033 == mqException.ReasonCode ) // there is no message on the queue
                    throw new MqNoMessageException( errorMessage );
                else
                {
                    errorMessage = String.Format
                    (
                        "getMessageWithSyncPoint(): reasonCode={0}, details={1}",
                        mqException.ReasonCode,
                        mqException.ToString()
                    );

                    throw new Exception( errorMessage );
                }
            }
            catch( Exception exception ) // some nasty error 2
            {
                throw new Exception( exception );
            }
        }

        public void GetMessageWithSyncPoint( ref string queuName, ref byte[] messageContent, int timeOut )
        {
            IBM.WMQ.MQMessage mqMessage = null;
            getMessageWithSyncPoint( ref queuName, ref mqMessage, timeOut );

            messageContent = mqMessage.ReadBytes( mqMessage.DataLength );
        }

        public void GetMessageWithSyncPoint( ref string queuName, ref string messageContent, int timeOut )
        {
            IBM.WMQ.MQMessage mqMessage = null;
            getMessageWithSyncPoint( ref queuName, ref mqMessage, timeOut );

            byte[] messageContentAsByteArray = mqMessage.ReadBytes( mqMessage.DataLength );
            messageContent = Utils.byteArrayToString( ref messageContentAsByteArray );
        }

        public void PutMessageWithSyncPoint( ref string queuName, ref IBM.WMQ.MQMessage mqMessage )
        {
            if( false == _existingQueues.ContainsKey( queuName ) )
                return;

            try
            {
                IBM.WMQ.MQPutMessageOptions mqPutMessageOptions         = new IBM.WMQ.MQPutMessageOptions();
                mqPutMessageOptions.Options|= IBM.WMQ.MQC.MQPMO_SYNCPOINT;

                IBM.WMQ.MQQueue	queue = (IBM.WMQ.MQQueue) _existingQueues[ queuName ];
                queue.Put( mqMessage, mqPutMessageOptions );
            }

            // some nasty error 1
            catch( IBM.WMQ.MQException mqException )
            {
                String errorMessage = mqException.ToString();
                throw new Exception( errorMessage, mqException );
            }

            // some nasty error 2
            catch( Exception exception )
            {
                String errorMessage = exception.ToString();
                throw new Exception( errorMessage, exception );
            }
        }

        public void PutMessageWithSyncPoint( ref string queuName, ref byte[] messageContent )
        {
            IBM.WMQ.MQMessage   mqMessage = new IBM.WMQ.MQMessage();
            mqMessage.Write( messageContent );

            putMessageWithSyncPoint( ref queuName, ref mqMessage );
        }

        public void PutMessageWithSyncPoint( ref string queuName, ref string messageContent )
        {
            byte[] messageContentAsByteArray = Utils.stringToByteArray( ref messageContent );

            putMessageWithSyncPoint( ref queuName, ref messageContentAsByteArray );
        }


        public void PutMessageWithNoSyncPoint( ref string queueName, ref IBM.WMQ.MQMessage mqMessage )
        {
            if( false == _existingQueues.ContainsKey( queueName ) )
                return;

            try
            {
                IBM.WMQ.MQPutMessageOptions mqPutMessageOptions         = new IBM.WMQ.MQPutMessageOptions();
                mqPutMessageOptions.Options|= IBM.WMQ.MQC.MQPMO_NO_SYNCPOINT;

                IBM.WMQ.MQQueue	queue = (IBM.WMQ.MQQueue) _existingQueues[ queueName ];
                queue.Put( mqMessage, mqPutMessageOptions );
            }

                // some nasty error 1
            catch( IBM.WMQ.MQException mqException )
            {
                String errorMessage = mqException.ToString();
                throw new Exception( errorMessage, mqException );
            }

                // some nasty error 2
            catch( Exception exception )
            {
                String errorMessage = exception.ToString();
                throw new Exception( errorMessage, exception );
            }
        }

        public void PutMessageWithNoSyncPoint( ref string queuName, ref byte[] messageContent )
        {
            IBM.WMQ.MQMessage   mqMessage = new IBM.WMQ.MQMessage();
            mqMessage.Write( messageContent );

            putMessageWithNoSyncPoint( ref queuName, ref mqMessage );
        }

        public void PutMessageWithNoSyncPoint( ref string queuName, ref string messageContent )
        {
            byte[] messageContentAsByteArray = Utils.stringToByteArray( ref messageContent );

            putMessageWithNoSyncPoint( ref queuName, ref messageContentAsByteArray );
        }

        public void DeleteMessageWithSyncPoint( ref string queueName, ref byte[] corelationId )
        {
            if( false == _existingQueues.ContainsKey( queueName ) )
                return;

            try
            {
                IBM.WMQ.MQGetMessageOptions mqGetMessageOptions = new IBM.WMQ.MQGetMessageOptions();
                mqGetMessageOptions.Options|= IBM.WMQ.MQC.MQGMO_SYNCPOINT;
                mqGetMessageOptions.Options|= IBM.WMQ.MQC.MQGMO_WAIT;
                mqGetMessageOptions.WaitInterval = 1000;
                mqGetMessageOptions.Options|= IBM.WMQ.MQC.MQMO_MATCH_CORREL_ID;

                IBM.WMQ.MQMessage   mqMessage = new IBM.WMQ.MQMessage();
                mqMessage.CorrelationId = corelationId;

                IBM.WMQ.MQQueue	queue = (IBM.WMQ.MQQueue) _existingQueues[ queueName ];
                queue.Get( mqMessage, mqGetMessageOptions );
            }
            catch( IBM.WMQ.MQException mqException ) // some nasty error 1
            {
                string errorMessage = mqException.ToString();

                if( 2033 == mqException.ReasonCode ) // there is no message on the queue
                    throw new MqNoMessageException( errorMessage );
                else
                {
                    errorMessage = String.Format
                    (
                        "getMessageWithSyncPoint(): reasonCode={0}, details={1}",
                        mqException.ReasonCode,
                        mqException.ToString()
                    );

                    throw new Exception( errorMessage );
                }
            }
            catch( Exception exception ) // some nasty error 2
            {
                throw new Exception( exception );
            }
        }

        public void Commit()
        {
            _mqQueueManager.Commit();
        }

        public void RollBack()
        {
            _mqQueueManager.Backout();
        }

        private void GetMessageContentIntoByteArray( ref IBM.WMQ.MQMessage mqMessage, ref byte[] messageContentAsByteArray )
        {
            messageContentAsByteArray = mqMessage.ReadBytes( mqMessage.DataLength );
        }

        private void GetMessageContentIntoString( ref IBM.WMQ.MQMessage mqMessage, ref string messageContentAsString )
        {
            byte[] messageContentAsByteArray = null;
            getMessageContentIntoByteArray( ref mqMessage, ref messageContentAsByteArray );

            messageContentAsString = Utils.byteArrayToString( ref messageContentAsByteArray );
        }

        public int GetQueuBackoutLimit( ref string queueName )
        {
            if( false == _existingQueues.ContainsKey( queueName ) )
                return -1;

            int[]   selectors   = { IBM.WMQ.MQC.MQIA_BACKOUT_THRESHOLD, IBM.WMQ.MQC.MQCA_BACKOUT_REQ_Q_NAME };
            int[]   intAttrs    = new int[ 1 ];
            byte[]  charAttrs   = new byte[ IBM.WMQ.MQC.MQ_Q_NAME_LENGTH ];

            IBM.WMQ.MQQueue	queue = (IBM.WMQ.MQQueue) _existingQueues[ queueName ];
            queue.Inquire( selectors, intAttrs, charAttrs );

            return intAttrs[ 0 ];
        }

        public string GetBackoutQueueNameForQueue( ref string queueName )
        {
            if( false == _existingQueues.ContainsKey( queueName ) )
                return string.Empty;

            int[]   selectors   = { IBM.WMQ.MQC.MQIA_BACKOUT_THRESHOLD, IBM.WMQ.MQC.MQCA_BACKOUT_REQ_Q_NAME };
            int[]   intAttrs    = new int[ 1 ];
            byte[]  charAttrs   = new byte[ IBM.WMQ.MQC.MQ_Q_NAME_LENGTH ];

            IBM.WMQ.MQQueue	queue = (IBM.WMQ.MQQueue) _existingQueues[ queueName ];
            queue.Inquire( selectors, intAttrs, charAttrs );

            string qBackoutName = NovadisTools.PdfFactory.BiteToString( charAttrs ).Trim();

            return qBackoutName;
        }
    }
}
