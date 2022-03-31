package com.chart;

import java.util.List;

import javax.swing.JFrame;  
import javax.swing.SwingUtilities;  
  
import org.jfree.chart.ChartFactory;  
import org.jfree.chart.ChartPanel;  
import org.jfree.chart.JFreeChart;  
import org.jfree.data.category.DefaultCategoryDataset;  

  
public class LineChart extends JFrame {  
  
	private static final long serialVersionUID = 1L;  
  
	private DefaultCategoryDataset dataset;
  
	public LineChart(List<Long> costTimes) {  
		super("Line Chart");
		createDataset(costTimes);
		createChart();
  }    
  
  
  private void createChart() {
	    // Create chart  
	    JFreeChart chart = ChartFactory.createLineChart(  
	        "Network Delay", // Chart title  
	        "Requests", // X-Axis Label  
	        "Delay/ms", // Y-Axis Label  
	        dataset  
	        );  
	  
	    ChartPanel panel = new ChartPanel(chart);  
	    setContentPane(panel);  
  }
  
  private void createDataset(List<Long> costTimes) {
	  String series1 = "Cost Times";
	  
	  dataset = new DefaultCategoryDataset(); 
	  
	  for(int i=0;i<costTimes.size();i++) {
		  dataset.addValue(costTimes.get(i), series1,(Comparable)i);
	  }

  }

  
}  