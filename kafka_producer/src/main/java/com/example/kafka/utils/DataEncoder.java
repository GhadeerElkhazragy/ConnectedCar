package com.example.kafka.utils;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.kafka.vd.VehicleData;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/* 
 * Convert VehicleData java object to JSON String
 * */

public class DataEncoder implements Encoder<VehicleData> {
	private static final Logger logger = Logger.getLogger(DataEncoder.class);	
	private static ObjectMapper objectMapper = new ObjectMapper();		
	public DataEncoder(VerifiableProperties verifiableProperties) {

    }
	public byte[] toBytes(VehicleData iotEvent) {
		try {
			String msg = objectMapper.writeValueAsString(iotEvent);
			logger.info(msg);
			return msg.getBytes();
		} catch (JsonProcessingException e) {
			logger.error("Error in Serialization", e);
		}
		return null;
	}

}
