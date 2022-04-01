package com.pbft;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import com.chart.LineChart;
import com.timemanager.*;
  

public class PbftMain {

	static Logger logger = LoggerFactory.getLogger(PbftMain.class);
	
	public static final int SIZE = 10;	
	public static final int LIMITE_SIZE = 25; //CPU在30左右超载
	public static final int REQUEST_NUM = 10;

	
	private static long[][] delayNet = new long[LIMITE_SIZE][LIMITE_SIZE];	
	
	private static Random r = new Random();	
	
	private static List<Long> costTimes = new ArrayList<>(); 
	
	private static List<Pbft> nodes = Lists.newArrayList();
	
	
	public static void main(String[] args) throws InterruptedException {
		
		//初始化网络延迟
		for(int i=0;i<SIZE;i++){
			for(int j=0;j<SIZE;j++){
				if(i != j){
					// 随机延时
					delayNet[i][j] = RandomUtils.nextLong(10, 60);
				}else{
					delayNet[i][j] = 10;
				}
			}
		}	  
		
		//多线程启动网络节点
		for(int i=0;i<SIZE;i++){
			nodes.add(new Pbft(i,SIZE).start());
		}
		
		//全网节点随机产生请求
		for(int i=0;i<REQUEST_NUM;i++){
			int node = r.nextInt(SIZE);
			nodes.get(node).req("test"+i);
		}

		Thread.sleep(3000);
		
		
		//console按编号输出执行时间
		System.out.println("请求运行时长：");
		for(int i=0;i<costTimes.size();i++) {			
			System.out.println(costTimes.get(i));
		}
		//平均执行时间
		long total = 0;
		for(int i=0;i<costTimes.size();i++) {	
			total += costTimes.get(i);
		}
		System.out.println("平均执行时间：" + total/costTimes.size());
		
		//绘制图表
    	LineChart example = new LineChart(costTimes);
	    SwingUtilities.invokeLater(() -> {    
			example.setAlwaysOnTop(false);  
			example.pack();  
			example.setSize(600, 400);  
			example.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);  
			example.setVisible(true);  
	    });  

	    
	
	}

	/**
	 * 广播消息
	 * @param msg
	 */
	public static void publish(PbftMsg msg){
		//logger.info("publish广播消息[" +msg.getNode()+"]:"+ msg);
		for(Pbft pbft:nodes){
			// 模拟网络时延
			TimerManager.schedule(()->{
				pbft.push(new PbftMsg(msg));
				return null;
			}, delayNet[msg.getNode()][pbft.getIndex()]);
		}
	}
	
	/**
	 * 发送消息到指定节点
	 * @param toIndex
	 * @param msg
	 */	
	public static void send(int toIndex,PbftMsg msg){
		// 模拟网络时延
		TimerManager.schedule(()->{
			nodes.get(toIndex).push(new PbftMsg(msg));
			return null;
		}, delayNet[msg.getNode()][toIndex]);
	}
	
	public static void collectTimes(long costTime) {
		costTimes.add(costTime);
	}	

	
}