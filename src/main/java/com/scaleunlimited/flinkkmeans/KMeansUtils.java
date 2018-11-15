package com.scaleunlimited.flinkkmeans;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class KMeansUtils {

    public static List<Feature> makeFeatures(InputStream is) throws ParseException, IOException {
        return makeFeatures(is, Integer.MAX_VALUE);
    }
    
    public static List<Feature> makeFeatures(InputStream is, int maxFeatures) throws ParseException, IOException {
        List<Feature> features = new ArrayList<>();
        
        // Get data, in <date time><tab><lat><tab><lon> format.
        // 2018-08-01 00:00:07.3210 40.78339981 -73.98093133
        List<String> rides = IOUtils.readLines(is, StandardCharsets.UTF_8);
        
        SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS");
        
        int rideID = 0;
        for (String ride : rides) {
            String[] fields = ride.split("\t", 3);
            Date startTime = parser.parse(fields[0]);
            double lat = Double.parseDouble(fields[1]);
            double lon = Double.parseDouble(fields[2]);

            // x is longitude, y is latitude
            Feature f = new Feature(rideID++, lon, lat);
            f.setTimestamp(startTime.getTime());
            features.add(f);
            
            if (rideID >= maxFeatures) {
                break;
            }
        }
        
        return features;
    }

    public static double calcMaxDistance(List<Feature> features, int numClusters) {
        double minX = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE;
        double minY = Double.MAX_VALUE;
        double maxY = -Double.MAX_VALUE;
        
        for (Feature f : features) {
            minX = Math.min(minX, f.getX());
            maxX = Math.max(maxX, f.getX());
            minY = Math.min(minY, f.getY());
            maxY = Math.max(maxY, f.getY());
        }
        
        double xDelta = maxX - minX;
        double yDelta = maxY - minY;
        return Math.sqrt((xDelta * xDelta) + (yDelta * yDelta)) / numClusters;
    }

}
