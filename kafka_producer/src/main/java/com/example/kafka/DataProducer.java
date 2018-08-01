package com.example.kafka;

import java.util.ArrayList;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.example.kafka.utils.PropertyFileReader;
import com.example.kafka.vd.VehicleData;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class DataProducer {
	
	private static final Logger logger = Logger.getLogger(DataProducer.class);

	public static void main(String[] args) throws Exception {
		//read config file
		Properties prop = PropertyFileReader.readPropertyFile();	
		String zookeeper = prop.getProperty("com.iot.app.kafka.zookeeper");
		String brokerList = prop.getProperty("com.iot.app.kafka.brokerlist");
		String topic = prop.getProperty("com.iot.app.kafka.topic");
		logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + brokerList + " and topic " + topic);

		// set producer properties
		Properties properties = new Properties();
		properties.put("zookeeper.connect", zookeeper);
		properties.put("metadata.broker.list", brokerList);
		properties.put("request.required.acks", "1");
		properties.put("serializer.class", "com.example.kafka.utils.DataEncoder");
		//generate event
		Producer<String, VehicleData> producer = new Producer<String, VehicleData>(new ProducerConfig(properties));
		DataProducer iotProducer = new DataProducer();
		iotProducer.generateIoTEvent(producer,topic);		
	}
	
			/*
			 Runs in while loop and generates random IoT data in JSON.
			*/
	
		private void generateIoTEvent(Producer<String, VehicleData> producer, String topic) throws InterruptedException {
			List<String> routeList = Arrays.asList(new String[]{"Route-37", "Route-43", "Route-82"});
			List<String> vehicleTypeList = Arrays.asList(new String[]{"Car 1", "Car 2", "Car 3"});
			String vehicleId1 = UUID.randomUUID().toString();
			String vehicleId2 = UUID.randomUUID().toString();
			String vehicleId3= UUID.randomUUID().toString();
			String vehicleId=null;
				
				Random rand = new Random();
				logger.info("Sending events");
				// generate event in loop
				while (true) {
					List<VehicleData> eventList = new ArrayList<VehicleData>();
					for (int i = 0; i < 100; i++) {
						
						String vehicleType = vehicleTypeList.get(rand.nextInt(3));
						if (vehicleType == "Car 1") {vehicleId = vehicleId1;}
						else if (vehicleType == "Car 2") {vehicleId = vehicleId2;}
						else if (vehicleType == "Car 3") {vehicleId = vehicleId3;}
						String routeId = null;
						if (vehicleType == "Car 1") {routeId = "Route-37";}
						else if (vehicleType == "Car 2") {routeId = "Route-43";}
						else if (vehicleType == "Car 3") {routeId = "Route-82";}
						Date timestamp = new Date();
						double speed = rand.nextInt(100 - 20) + 20;// random speed between 20 to 100
						double fuelLevel = rand.nextInt(40 - 10) + 10;
						for (int j = 0; j < 5; j++) {// Add 5 events for each vehicle
							String coords = getCoordinates(routeId);
							String latitude = coords.substring(0, coords.indexOf(","));
							String longitude = coords.substring(coords.indexOf(",") + 1, coords.length());
							VehicleData event = new VehicleData(vehicleId, vehicleType, routeId, latitude, longitude, timestamp, speed,fuelLevel);
							eventList.add(event);
						}
					}
					Collections.shuffle(eventList);// shuffle for random events
					for (VehicleData event : eventList) {
						KeyedMessage<String, VehicleData> data = new KeyedMessage<String, VehicleData>(topic, event);
						producer.send(data);
						Thread.sleep(rand.nextInt(3000 - 1000) + 1000);//random delay of 1 to 3 seconds
					}
				}
			}
			
			//generate random latitude and longitude for routes
			private String  getCoordinates(String routeId) {
				Random rand = new Random();
				int latPrefix = 0;
				int longPrefix = -0;
				if (routeId.equals("Route-37")) {
					latPrefix = 33;
					longPrefix = -96;
				} else if (routeId.equals("Route-82")) {
					latPrefix = 34;
					longPrefix = -97;
				} else if (routeId.equals("Route-43")) {
					latPrefix = 35;
					longPrefix = -98;
				} 
				Float lati = latPrefix + rand.nextFloat();
				Float longi = longPrefix + rand.nextFloat();
				return lati + "," + longi;
			}

	}
