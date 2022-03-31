package com.HQ;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import javax.swing.SwingUtilities;
import javax.swing.WindowConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.RandomUtils;

import com.chart.LineChart;
import com.google.common.collect.Lists;
import com.timemanager.*;

public class HQMain {
	
	static Logger logger = LoggerFactory.getLogger(HQMain.class);
	
	public static final int SIZE =100;	
	public static final int CREDIT_LEVEL = 60;	//总分：100
	public static final int MIN_CONSENSUS_NUM = 4;  //最小共识节点数
	public static final int MAX_CONSENSUS_NUM = 20;  //最大共识节点数
	public static final int REQUEST_NUM = 10;
	
	private static List<HQ> nodes = Lists.newArrayList();
	
	private static List<HQ> consensusNodes = Lists.newArrayList(); 
	private static List<HQ> candidateNodes = Lists.newArrayList(); 
	
	private static Random r = new Random();	
	
	private static long[][] delayNet = new long[SIZE][SIZE];	
	
	private static List<Long> costTimes = new ArrayList<>(); 

	
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
		
		//创建网络节点
		for(int i=0;i<SIZE;i++){
			nodes.add(new HQ(i,SIZE,false,true));
		}
		
		//共识节点,候选节点分类 并传入HQ类静态变量
		classifyNodes(CREDIT_LEVEL,MAX_CONSENSUS_NUM);	
		HQ.setHQSize(consensusNodes.size());
		
		//多线程启动网络共识节点
		if (consensusNodes.size() < MIN_CONSENSUS_NUM) {
			System.out.println("Unenough creditable nodes, there are less than "
					+ MIN_CONSENSUS_NUM + "creditable nodes!");
		}else {		
			for(int i=0;i<consensusNodes.size();i++) {
				consensusNodes.get(i).start();
			}	
		}
		
		//全网节点随机产生请求
		for(int i=0;i<REQUEST_NUM;i++){
			int node = r.nextInt(SIZE);
			nodes.get(node).req("test"+i);
		}
		
		
		
		Thread.sleep(5000);
		
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
	
	
	//取满足可信度水平且不超过最大共识节点数的所有节点
	public static void classifyNodes(int creditLevel, int maxQuantity) {
//		Collections.reverse(nodes);   //集合降序排列----暂时不用
		for(int i=0;i<SIZE;i++) {
			int num = 0;
			if(nodes.get(i).getCredit() > creditLevel && num < maxQuantity) {
				consensusNodes.add(nodes.get(i));
				num++;
			}else {
				candidateNodes.add(nodes.get(i));
			}
		}
	}			
	
	public static void Bstart() {
		for(int i=0;i<candidateNodes.size();i++) {
			//candidateNodes.get(i).setHQ(true);
			candidateNodes.get(i).start();
		}
	}

	/**
	 * 向HQ节点广播消息
	 * @param msg
	 */
	public static void HQpublish(HQMsg msg){
		//logger.info("HQpublish广播消息[" +msg.getNode()+"]:"+ msg);
		for(HQ hq:consensusNodes){
			// 模拟网络时延
			TimerManager.schedule(()->{
				hq.push(new HQMsg(msg));
				return null;
			}, delayNet[msg.getNode()][hq.getIndex()]);
		}
	}

	
	/**
	 * 向所有节点广播消息
	 * @param msg
	 */
	public static void publish(HQMsg msg){
//		logger.info("publish广播消息[" +msg.getNode()+"]:"+ msg);
		for(HQ hq:nodes){
			// 模拟网络时延
			TimerManager.schedule(()->{
				hq.push(new HQMsg(msg));
				return null;
			}, delayNet[msg.getNode()][hq.getIndex()]);
		}
	}
	
	/**
	 * 发送消息到指定节点
	 * @param toIndex
	 * @param msg
	 */	
	public static void send(int toIndex,HQMsg msg){
		// 模拟网络时延		
		TimerManager.schedule(()->{
			nodes.get(toIndex).push(msg);
			return null;
		}, delayNet[msg.getNode()][toIndex]);
	}	

	//收集Request处理时长
	public static void collectTimes(long costTime) {
		costTimes.add(costTime);
	}	

}
