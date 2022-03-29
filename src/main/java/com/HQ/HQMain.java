package com.HQ;

import java.util.List;
import java.util.Collections;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jfree.chart.*;

import com.google.common.collect.Lists;
import com.pbft.TimerManager;

public class HQMain {
	
	static Logger logger = LoggerFactory.getLogger(HQMain.class);
	
	public static final int SIZE =10;	
	public static final int CREDIT_LEVEL = 3;
	public static final int MIN_CONSENSUS_NUM = 5;  //最小共识节点数
	public static final int MAX_CONSENSUS_NUM = SIZE/2;  //最大共识节点数
	
	private static List<HQ> nodes = Lists.newArrayList();
	
	private static List<HQ> consensusNodes = Lists.newArrayList(); 
	private static List<HQ> candidateNodes = Lists.newArrayList(); 
	
	private static Random r = new Random();
	
	private static long[] net = new long[9999];
	
	public static void main(String[] args) throws InterruptedException {
		
		for(int i=0;i<SIZE;i++){
			nodes.add(new HQ(i,SIZE,false,true));
		}

		start();			

		//nodes.get(1).setByzt();
		// 初始化模拟网络
		for(int i=0;i<SIZE;i++){
			for(int j=0;j<SIZE;j++){
				if(i != j){
					// 随机延时
					net[i*10+j] = 10;
				}else{
					net[i*10+j] = 5;
				}
			}
		}		
		
		// 模拟请求端发送请求
		for(int i=0;i<1;i++){
			int node = r.nextInt(SIZE);
			nodes.get(node).req("test"+i);
		}
		
//		Thread.sleep(10000);
//		System.out.println("9--------------------------------------------------------");
//		// 1秒后，主节点宕机
//		nodes.get(0).close();
//		for(int i=2;i<4;i++){
//			nodes.get(i).req("testD"+i);
//		}
//		//1秒后恢复
//		Thread.sleep(1000);
//		System.out.println("9--------------------------------------------------------");
//
//		nodes.get(0).back();
//		for(int i=1;i<2;i++){
//			nodes.get(i).req("testB"+i);
//		}		
	}
	
	
	public static void classifyNodes(int creditLevel, int maxQuantity) {
		Collections.reverse(nodes);
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

	
	public static void start() {

		classifyNodes(CREDIT_LEVEL,MAX_CONSENSUS_NUM);		

		if (consensusNodes.size() < MIN_CONSENSUS_NUM) {
			System.out.println("Unenough creditable nodes, there are less than "
					+ MIN_CONSENSUS_NUM + "creditable nodes!");
		}else {		
			for(int i=0;i<consensusNodes.size();i++) {
				consensusNodes.get(i).start();
			}	
		}
		return;
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
			}, net[msg.getNode()*10+hq.getIndex()]);
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
			}, net[msg.getNode()*10+hq.getIndex()]);
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
		}, net[msg.getNode()*10+toIndex]);
	}	


}
