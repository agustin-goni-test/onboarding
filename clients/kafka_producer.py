from confluent_kafka import Producer, KafkaException
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field
import json
from logger import Logger

logger = Logger()

class KafkaConfig(BaseModel):
    '''Explicit configuration for the server'''
    bootstrap_servers: str = Field(alias='bootstrap.servers')
    security_protocol: str = Field(alias='security.protocol', default='SASL_SSL')
    sasl_mechanism: str = Field(alias='sasl.mechanism', default='PLAIN')
    sasl_username: str = Field(alias='sasl.username')
    sasl_password: str = Field(alias='sasl.password')
    client_id: str = Field(alias='client.id', default='python-producer')

    # Use Pydantic's configuration to allow mapping to Kafka's dot-notation keys
    class Config:
        allow_population_by_field_name = True
    

class ConfluentProducerClient:
    '''
    This class is a client that connects to Confluent and sends message.
    Singleton implementation.
    '''
    _instance: Optional['ConfluentProducerClient'] = None
    config: KafkaConfig
    _producer: Optional[Producer] = None

    def __new__(cls, config_dict: Dict[str, Any]):
        if cls._instance is None:
            cls._instance = super().__new__(cls)

            # Validate and create Pydantic config object
            config_object = KafkaConfig(**config_dict)
            cls._instance.config = config_object
            # cls._instance._producer = None
            cls._instance._connect()
        return cls._instance
    
    def _connect(self):
        '''Confluent Kafka initialization'''
        try:
            # Convert Pydantyic model back to dict for the Kafka client
            kafka_settings = self.config.model_dump(by_alias=True)

            # Now you can recognize self.config
            self._producer = Producer(kafka_settings)
            logger.info("Confluent Kafka Producer inicializado con éxito...")            

        except KafkaException as e:
            logger.error(f"Error al inicializar Kafka Producer: {e}")
            self._producer = None

    def _delivery_report(self, err, msg):
        '''Callback function called when message is produced.'''
        if err is not None:
            logger.error(f"Mensaje no pudo ser entregado: {err}")
        else:
            logger.info(f"Mensaje entregado con éxito a {msg.topic()} - {msg.partition()} con offset {msg.offset()}")


    def send_message(self, topic: str, key: str, value: Dict[str, Any]):
        '''
        Asynchronously sends a message to the specified Kafka topic.

        Args:
            topic (str): The name of the target Kafka topic.
            key (str): The message key (used for partitioning).
            value (Dict[str, Any]): The message payload (will be serialized to JSON).
        """
        '''

        if not self._producer:
            logger.error("No se puede mandar mensaje. Producer no está conectado.")
            return
        
        # Serialize the dictionary value to JSON
        value_bytes = json.dumps(value).encode('utf-8')
        key_bytes = key.encode('utf-8')

        try:
            # Asynchronously produce the message
            self._producer.produce(
                topic=topic,
                key=key_bytes,
                value=value_bytes,
                callback=self._delivery_report
            )
            self._producer.poll(0)

        except BufferError:
            logger.error("La cola del productor está llena... esperando...")
            self._producer.flush()  # Wait for queue to clear

        except Exception as e:
            logger.error(f"Ocurrió un error inesperado al producir el mensaje: {e}")

    def close(self):
        '''Waits for messages in the local queue to be delivered.'''
        if self._producer:
            logger.info("\nEliminando mensajes pendientes...")
            remaining_messages = self._producer.flush()
            logger.info(f"{remaining_messages} todavía en la cola después del flush (lo ideal es 0)")