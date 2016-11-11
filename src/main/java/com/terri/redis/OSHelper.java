/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terri.redis;
import java.lang.management.ManagementFactory;  
import com.sun.management.OperatingSystemMXBean; 
import java.io.File;
/**
 *
 * @author Terri
 */
public class OSHelper {
 
    public static double getFreePhysicalMemorySize(){ 
        OperatingSystemMXBean osmb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();  
         return  osmb.getFreePhysicalMemorySize() /1024/1024/1024.0;
    }
    
    
    public static void main(String[] args)   
    {  
        OperatingSystemMXBean osmb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();  
        System.out.println("系统物理内存总计：" + osmb.getTotalPhysicalMemorySize() / 1024/1024 + "MB");  
        System.out.println("系统物理可用内存总计：" + osmb.getFreePhysicalMemorySize() / 1024/1024 + "MB");   
        
        File[] roots = File.listRoots();//获取磁盘分区列表  
        for (File file : roots) {  
            System.out.println(file.getPath()+"信息如下:");  
            System.out.println("空闲未使用 = " + file.getFreeSpace()/1024/1024/1024+"G");//空闲空间  
            System.out.println("已经使用 = " + file.getUsableSpace()/1024/1024/1024+"G");//可用空间  
            System.out.println("总容量 = " + file.getTotalSpace()/1024/1024/1024+"G");//总空间  
            System.out.println();  
        }  
    }  
    
   }  

