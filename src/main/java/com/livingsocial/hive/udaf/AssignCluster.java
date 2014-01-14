package com.livingsocial.hive.udaf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class AssignCluster extends UDAF {
	
	//Number of clusters, probably should be a parameter somehow
	private static final int CLUSTER_SIZE = 128;

	/**
	 * The internal state of the aggregation. This keeps stores the similarity
	 * between the current person vector and each of the cluster centroids.
	 */
	public static class UDAFAssignClusterState{
		ArrayList<Double> similarity = new ArrayList<Double>(CLUSTER_SIZE);
	}
	
	/**
	 * Loads the cluster centroids from HDFS
	 * @return The cluster centroids, the array indices correspond to the
	 * cluster indices. That is, centroids[0] contains the vector for 
	 * cluster 0.
	 */
	 public static Map<String, Double>[] getCentroids(){
		 return DefaultSetHolder.DEFAULT_STOP_SET;
	 }
	 
	 /*
	  * Some magical lazy loading, centroids aren't read until 
	  * getCentoids() is called.
	  */
	 private static class DefaultSetHolder {
		      static final Map<String, Double>[] DEFAULT_STOP_SET = readCentroids();
	 }
	 
	 private static Map<String, Double>[] readCentroids() {
		 	@SuppressWarnings("unchecked")
			Map<String, Double>[] entries = new Map[CLUSTER_SIZE];
			for (int i = 0; i < CLUSTER_SIZE; i++) {
				entries[i] = new HashMap<String, Double>();
			}
			
			Configuration conf = new Configuration();
			String root = conf.get("fs.defaultFS");
			String path = root
					+ "/user/hive/warehouse/msperlich.db/daily_cluster_centroids128/centroids128_sparse.csv";
			try{
			FileSystem fs = FileSystem.get(conf);
			Path centroidsFile = new Path(path);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fs.open(centroidsFile)));
			while(reader.ready()) {
				String[] tokens = reader.readLine().split(",");
				int cluster = Integer.parseInt(tokens[0]);
				String word = tokens[1].intern();
				double weight = Double.parseDouble(tokens[2]);
				entries[cluster].put(word, weight);
			}
			}
			catch(IOException e){
				throw new RuntimeException("Unable to read cluster centrods", e);
			}
			
			return entries;

		}
	
	public static class UDAFAssignClusterEvaluator implements UDAFEvaluator {
		UDAFAssignClusterState state;
		
	    public UDAFAssignClusterEvaluator() {
	        super();
	        state = new UDAFAssignClusterState();
	        init();
	      }

	      /**
	       * Reset the state of the aggregation. 
	       * Sets all similarities to zero.
	       */
	      public void init() {
	    	  if(state.similarity.size() == 0){
	    		  for(int c = 0; c < CLUSTER_SIZE; c++){
		    		  state.similarity.add(0.0);
		    	  } 
	    	  }
	    	  else{
	    		  for(int c = 0; c < CLUSTER_SIZE; c++){
		    		  state.similarity.set(c, 0.0);
		    	  }
	    	  }
	      }

		/**
		 * Terminate a partial aggregation and return the state.
		 */
		public UDAFAssignClusterState terminatePartial() {
			return state;
		}

		/**
		 * Merge with a partial aggregation, similarities are added.
		 * 
		 */
		public boolean merge(UDAFAssignClusterState o) {
			for (int c = 0; c < CLUSTER_SIZE; c++) {
				state.similarity.set(c, state.similarity.get(c) + o.similarity.get(c));
			}
			return true;
		}

		public boolean iterate(String word, Double weight) {
			if (word != null && weight != null) {
				word = word.intern();
				Map<String, Double>[] centroids = getCentroids();
				for (int c = 0; c < CLUSTER_SIZE; c++) {
					Double clusterWeight = centroids[c].get(word);
					if (clusterWeight != null) {
						state.similarity.set(c,  state.similarity.get(c) +  clusterWeight * weight);
					}
				}
			}
			return true;
		}
		
		/**
		 * Return the index of the cluster with the highest similarity.
		 * @return
		 */
		public Integer terminate() {
			int maxIndex = 0;
			double maxSimilarity = Double.NEGATIVE_INFINITY;
			for (int c = 0; c < CLUSTER_SIZE; c++) {
				double similarity = state.similarity.get(c);
				if (similarity > maxSimilarity) {
					maxSimilarity = similarity;
					maxIndex = c;
				}
			}
			return maxIndex;
		}
	}
	
	private AssignCluster(){
	}
}
