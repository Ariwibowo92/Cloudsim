/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cloudbus.cloudsim.ta_ari;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.UtilizationModelNull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.network.datacenter.EdgeSwitch;
import org.cloudbus.cloudsim.network.datacenter.NetDatacenterBroker;
import org.cloudbus.cloudsim.network.datacenter.NetworkCloudlet;
import org.cloudbus.cloudsim.network.datacenter.NetworkConstants;
import org.cloudbus.cloudsim.network.datacenter.NetworkDatacenter;
import org.cloudbus.cloudsim.network.datacenter.NetworkHost;
import org.cloudbus.cloudsim.network.datacenter.NetworkVmAllocationPolicy;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

/**
 * finished time analysis
 */
public class FinishedTimeLeastConnection {

    /** The cloudlet list. */
    private static List<Cloudlet> cloudletList,cloudletListMinMax;

    /** The vmlist. */
    private static List<Vm> vmlist;

    private static List<Vm> createVM(int userId, int vms, int idShift) {
            //Creates a container to store VMs. This list is passed to the broker later
            LinkedList<Vm> list = new LinkedList<Vm>();

            //create VMs
            Vm[] vm = new Vm[vms];

            for(int i=0;i<vms;i++){
                //if (i<10)
                        //vm[i] = new Vm(idShift + i, userId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
                //else
                Random r = new Random();

                //VM Parameters
                long size = r.ints(1, 5000, 20000).findFirst().getAsInt();//10000; //image size (MB)
                int ram = r.ints(1, 100, 400).findFirst().getAsInt();//512; //vm memory (MB)
                int mips = r.ints(1, 100, 400).findFirst().getAsInt();//250;
                long bw = r.ints(1, 800, 1500).findFirst().getAsInt();//1000;
                int pesNumber = 1; //number of cpus
                String vmm = "Xen"; //VMM name
            
                vm[i] = new Vm(idShift + i, userId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
                list.add(vm[i]);
            }

            return list;
    }
    
    private static List<Cloudlet> createCloudlet(int userId, int cloudlets, int idShift){
        // Creates a container to store Cloudlets
        LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

        Cloudlet[] cloudlet = new Cloudlet[cloudlets];

        for(int i=0;i<cloudlets;i++){
            Random r = new Random();
//            if (i%2==0 || i<2)
//                length *=  5;
//            else 
//                length /=  10;
            //cloudlet parameters
            long length = r.ints(1, 100000, 300000).findFirst().getAsInt();//40000;
            long fileSize = r.ints(1, 200, 400).findFirst().getAsInt();//300;
            long outputSize = r.ints(1, 200, 400).findFirst().getAsInt();//300;
            int pesNumber = 1;
            UtilizationModel utilizationModel = new UtilizationModelFull();

            cloudlet[i] = new Cloudlet(idShift + i, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
            // setting the owner of these Cloudlets
            cloudlet[i].setUserId(userId);
            list.add(cloudlet[i]);
        }

        return list;
    }

    private static void getCloudletListMinMax(List<Cloudlet> clist)
    {
        //Min
        int min=0;
        for (int i=0; i<clist.size();i++)
                if (clist.get(i).getCloudletLength() < clist.get(min).getCloudletLength())
                        min=i;
        cloudletListMinMax.add(clist.get(min));
        clist.remove(min);
        if (clist.size()!=0)
                getCloudletListMinMax(clist);
    }

    private static void sortCloudletListMinMax(List<Cloudlet> clist)
    {
     Cloudlet c;
     for (int i=0; i<clist.size()-1; i++)
            //Max
            if (clist.get(i).getCloudletLength() > clist.get(i+1).getCloudletLength())
            {
                    c=clist.get(i);
                    clist.add(i, clist.get(i+1));
                    clist.add(i+1,c);
                    c=null;
            }
    }

    // method to find the average waiting time in a virtual machine
    private static double VmArt(List<Cloudlet> list, int VmId)
    {
            int c = 0;
            double art = 0; 
            for(int i=0;i<list.size();i++)
                    if (list.get(i).getVmId() == VmId)
                    {
                            art = art + list.get(i).getExecStartTime();    c++;
                    }
                    art =  art / c;
            return art;
    }

    private static double VmMakespane(List<Cloudlet> cloudlets, int VmId)
    {
            double mkspane = 0; 
            for(int i=0;i<cloudlets.size();i++)
                    if (cloudlets.get(i).getVmId() == VmId)
                            if (cloudlets.get(i).getFinishTime() > mkspane)
                                    mkspane =  cloudlets.get(i).getFinishTime(); 
            return mkspane;
    }
    
    private static Datacenter createDatacenter(String name){

            // Here are the steps needed to create a PowerDatacenter:
            // 1. We need to create a list to store one or more
            //    Machines
            List<Host> hostList = new ArrayList<Host>();

            // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
            //    create a list to store these PEs before creating
            //    a Machine.
            List<Pe> peList1 = new ArrayList<Pe>();

            int mips = 1000;

            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
            peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
            peList1.add(new Pe(2, new PeProvisionerSimple(mips)));
            peList1.add(new Pe(3, new PeProvisionerSimple(mips)));

            //Another list, for a dual-core machine
            List<Pe> peList2 = new ArrayList<Pe>();

            peList2.add(new Pe(0, new PeProvisionerSimple(mips)));
            peList2.add(new Pe(1, new PeProvisionerSimple(mips)));

            //4. Create Hosts with its id and list of PEs and add them to the list of machines
            int hostId=0;
            int ram = 16384; //host memory (MB)
            long storage = 1000000; //host storage
            int bw = 10000;

            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new VmSchedulerSpaceShared(peList1)
                    )
            ); // This is our first machine

            hostId++;

            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList2,
                            new VmSchedulerSpaceShared(peList2)
                    )
            ); // Second machine

            // 5. Create a DatacenterCharacteristics object that stores the
            //    properties of a data center: architecture, OS, list of
            //    Machines, allocation policy: time- or space-shared, time zone
            //    and its price (G$/Pe time unit).
            String arch = "x86";      // system architecture
            String os = "Linux";          // operating system
            String vmm = "Xen";
            double time_zone = 10.0;         // time zone this resource located
            double cost = 3.0;              // the cost of using processing in this resource
            double costPerMem = 0.05;		// the cost of using memory in this resource
            double costPerStorage = 0.1;	// the cost of using storage in this resource
            double costPerBw = 0.1;			// the cost of using bw in this resource
            LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

            DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
            arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


            // 6. Finally, we need to create a PowerDatacenter object.
            Datacenter datacenter = null;
            try {
                    //new VmAllocationPolicySimple(hostList)
                    LeastConnectionVmAllocationPolicy vm_policy = new LeastConnectionVmAllocationPolicy(hostList);
                    datacenter = new Datacenter(name, characteristics, vm_policy, storageList, 0);
            } catch (Exception e) {
                    e.printStackTrace();
            }
            
            return datacenter;
    }

    //We strongly encourage users to develop their own broker policies, to submit vms and cloudlets according
    //to the specific rules of the simulated scenario
    private static DatacenterBroker createBroker(String name) throws Exception{

        return new LeastConnectionDatacenterBroker(name);
//		DatacenterBroker broker = null;
//		try {
//			broker = new DatacenterBroker(name);
//		} catch (Exception e) {
//			e.printStackTrace();
//			return null;
//		}
//		return broker;
    }

    /**
     * Prints the Cloudlet objects
     * @param list  list of Cloudlets
     */
    private static void printCloudletList(List<Cloudlet> list) throws FileNotFoundException {
        String filename = new SimpleDateFormat("yyyyMMddHHmm'.csv'").format(new Date());
        PrintWriter pw = new PrintWriter(new File("FinishedTimeLeastConnection_RunResult_"+filename));
        StringBuilder sb = new StringBuilder();
        String line = "";
        
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        line = "Cloudlet ID" + "," + 
                "STATUS" + "," +
                "VM ID" + "," + 
                "Finish Time";
        sb.append(line);sb.append('\n');
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
                        "VM ID" + indent + indent + "Finish Time");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
                cloudlet = list.get(i);
                Log.print(indent + cloudlet.getCloudletId() + indent + indent + indent);
                if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
                    line = cloudlet.getCloudletId() + "," + 
                        "SUCCESS" + "," +
                        cloudlet.getVmId() + "," + 
                        dft.format(cloudlet.getFinishTime());
                    sb.append(line);sb.append('\n');
                    
                    Log.print("SUCCESS");
                    Log.printLine(
                            indent + indent + indent + cloudlet.getVmId() +
                            indent + indent + indent + dft.format(cloudlet.getFinishTime()));
                }
        }
        pw.write(sb.toString());
        pw.close();
    }

    /////////////////// MAIN METHOD ////////////////////
    public static void main(String[] args) {
        Log.printLine("Starting Simulation...");

        try {
            // First step: Initialize the CloudSim package. It should be called
            // before creating any entities.
            int num_user = 10;   // number of grid users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;  // mean trace events

            // Initialize the CloudSim library
            CloudSim.init(num_user, calendar, trace_flag);

            //GlobalBroker globalBroker = new GlobalBroker("GlobalBroker");

            // Second step: Create Datacenters
            //Datacenters are the resource providers in CloudSim. We need at list one of them to run a CloudSim simulation
            Datacenter datacenter0 = createDatacenter("Datacenter_0");

            //Third step: Create Broker
            //DatacenterBroker broker = createBroker("Broker_0");
            DatacenterBroker broker = createBroker("Broker_0");
            int brokerId = broker.getId();

            //Fourth step: Create VMs and Cloudlets and send them to broker
            vmlist = createVM(brokerId, 5, 1); //creating 5 vms
            cloudletList = createCloudlet(brokerId, 300, 1); // creating 300 cloudlets

            Log.printLine("cloudletlist size = " + cloudletList.size());
            cloudletListMinMax = new LinkedList<Cloudlet>();
            getCloudletListMinMax(cloudletList);    
            sortCloudletListMinMax(cloudletList);		

            broker.submitVmList(vmlist);
            //broker.submitCloudletList(cloudletList);
            broker.submitCloudletList(cloudletListMinMax);

            for (Cloudlet cloudlet:cloudletListMinMax)
            //for(Cloudlet cloudlet:cloudletList)
            {
                if (cloudlet.getCloudletId() % 5 == 0)
                {
                        broker.bindCloudletToVm(cloudlet.getCloudletId(), 5);  //first parameter is cloudletid and second is vm id
                        Log.printLine("Cloudlet with id " + cloudlet.getCloudletId() +" is assigned to vm with id 4");
                }
                else if (cloudlet.getCloudletId() % 4 == 0)
                {
                        broker.bindCloudletToVm(cloudlet.getCloudletId(), 4);  //first parameter is cloudletid and second is vm id
                        Log.printLine("Cloudlet with id " + cloudlet.getCloudletId() +" is assigned to vm with id 3");
                }
                else if (cloudlet.getCloudletId() % 3 == 0)
                {
                        broker.bindCloudletToVm(cloudlet.getCloudletId(), 3);  //first parameter is cloudletid and second is vm id
                        Log.printLine("Cloudlet with id " + cloudlet.getCloudletId() +" is assigned to vm with id 2");
                }
                else if (cloudlet.getCloudletId() % 2 == 0)
                {
                        broker.bindCloudletToVm(cloudlet.getCloudletId(), 1);  //first parameter is cloudletid and second is vm id
                        Log.printLine("Cloudlet with id " + cloudlet.getCloudletId() +" is assigned to vm with id 1");
                }
                else
                {
                        broker.bindCloudletToVm(cloudlet.getCloudletId(), 2);  //first parameter is cloudletid and second is vm id
                        Log.printLine("Cloudlet with id " + cloudlet.getCloudletId() +" is assigned to vm with id 0");
                }
            }

            // Fifth step: Starts the simulation
            CloudSim.startSimulation();

            // Final step: Print results when simulation is over
            List<Cloudlet> newList = broker.getCloudletReceivedList();

            //newList.addAll(globalBroker.getBroker().getCloudletReceivedList());

            CloudSim.stopSimulation();

            printCloudletList(newList);

            //Print the debt of each user to each datacenter
            //datacenter0.printDebts();

            Log.printLine("CloudSim simulation finished!");

        }
        catch (Exception e)
        {
            e.printStackTrace();
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }
    
    private static double getCpuLoad(Cloudlet cloudlet) {
        double returnData = 0.0;
        returnData = cloudlet.GetCpuLoad(cloudlet.getActualCPUTime());
        return returnData;
    }
}

