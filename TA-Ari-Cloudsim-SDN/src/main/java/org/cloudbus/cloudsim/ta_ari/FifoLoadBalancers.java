package org.cloudbus.cloudsim.ta_ari;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class FifoLoadBalancers {
  
    /** The cloudlet list. */
    private static List<Cloudlet> cloudletList;

    /** The vmlist. */
    private static List<Vm> vmlist;

    private static List<Vm> createVM(int userId, int vms, int idShift) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<Vm> list = new LinkedList<Vm>();

        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 128; //vm memory (MB)
        int mips = 125;
        long bw = 1000;
        int pesNumber = 1; //number of cpus
        String vmm = "NEON"; //VMM name

        //create VMs
        Vm[] vm = new Vm[vms];

        for(int i=0;i < vms;i++){
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

                //cloudlet parameters
                long length = r.ints(1, 3000, 500000).findFirst().getAsInt();
                long fileSize = r.ints(1, 200, 400).findFirst().getAsInt();
                long outputSize = r.ints(1, 200, 400).findFirst().getAsInt();
                int pesNumber = 1;
                UtilizationModel utilizationModel = new UtilizationModelFull();

                cloudlet[i] = new Cloudlet(idShift + i, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
                // setting the owner of these Cloudlets
                cloudlet[i].setUserId(userId);
                list.add(cloudlet[i]);
            }

            return list;
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
        // 1. We need to create a list to store
        //    our machine
        List<Host> hostList = new ArrayList<Host>();

        // 2. A Machine contains one or more PEs or CPUs/Cores.
        // In this example, it will have only one core.
        List<Pe> peList = new ArrayList<Pe>();

        int mips = 102400;

        // 3. Create PEs and add these into a list.
        peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating

        //4. Create Host with its id and list of PEs and add them to the list of machines
        int hostId=0;
        int ram = 102400; //host memory (MB)
        long storage = 1000000; //host storage
        int bw = 200000;

        hostList.add(
                        new Host(
                                hostId,
                                new RamProvisionerSimple(ram),
                                new BwProvisionerSimple(bw),
                                storage,
                                peList,
                                new VmSchedulerTimeShared(peList)
                        )
                ); // This is our machine


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
        double costPerStorage = 0.001;	// the cost of using storage in this resource
        double costPerBw = 0.0;			// the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


        // 6. Finally, we need to create a PowerDatacenter object.
        Datacenter datacenter = null;
        try {
            datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datacenter;
    }

    private static DatacenterBroker createBroker(String name){
        DatacenterBroker broker = null;
        try {
                broker = new DatacenterBroker(name);
        } catch (Exception e) {
                e.printStackTrace();
                return null;
        }
        return broker;
    }

    /////////////////// HELPERS METHOD ////////////////////
    /**
     * Prints the Cloudlet objects
     * @param list  list of Cloudlets
     */
    private static void printCloudletList(List<Cloudlet> list) throws FileNotFoundException {
        String filename = new SimpleDateFormat("yyyyMMddHHmm'.csv'").format(new Date());
        PrintWriter pw = new PrintWriter(new File("FifoLoadBalancers_RunResult_"+filename));
        StringBuilder sb = new StringBuilder();
        String line = "";
        
        int size = list.size();
        Cloudlet cloudlet;

        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        line = "Cloudlet ID" + "," + 
                "STATUS" + "," +
                "Data center ID" + "," + 
                "VM ID" + "," + 
                "Processing Time" + "," + 
                "Start Time" + "," + 
                "Finish Time" + "," + 
                "CPU Load";
        sb.append(line);sb.append('\n');
        Log.printLine("Cloudlet ID" + indent + 
                "STATUS" + indent +
                "Data center ID" + indent + 
                "VM ID" + indent + indent + 
                "Processing Time" + indent + 
                "Start Time" + indent + 
                "Finish Time" + indent + 
                "CPU Load");

        DecimalFormat dft = new DecimalFormat("###.##");
        for (int i = 0; i < size; i++) {
                cloudlet = list.get(i);
                Log.print(indent + cloudlet.getCloudletId() + indent + indent);
                double cpuLoad = getCpuLoad(cloudlet);
                
                if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
                    line = cloudlet.getCloudletId() + "," + 
                        "SUCCESS" + "," +
                        cloudlet.getResourceId() + "," + 
                        cloudlet.getVmId() + "," + 
                        dft.format(cloudlet.getActualCPUTime()) + "," + 
                        dft.format(cloudlet.getExecStartTime()) + "," + 
                        dft.format(cloudlet.getFinishTime()) + "," + 
                        dft.format(cpuLoad);
                    sb.append(line);sb.append('\n');
                    
                    Log.print("SUCCESS");
                    Log.printLine( indent + indent + indent + 
                            cloudlet.getResourceId() 
                            //cloudlet.getUserId()
                            + indent + indent + indent + cloudlet.getVmId()
                            + indent + indent + indent + dft.format(cloudlet.getActualCPUTime())
                            + indent + indent + indent + dft.format(cloudlet.getExecStartTime())
                            + indent + indent + indent + dft.format(cloudlet.getFinishTime())
                            + indent + indent + dft.format(cpuLoad));
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
                DatacenterBroker broker = createBroker("Broker_0");
                int brokerId = broker.getId();

                //Fourth step: Create VMs and Cloudlets and send them to broker
                vmlist = createVM(brokerId, 5, 0); //creating 5 vms
                cloudletList = createCloudlet(brokerId, 300, 0); // creating 300 cloudlets

                broker.submitVmList(vmlist);
                broker.submitCloudletList(cloudletList);

                // Fifth step: Starts the simulation
                CloudSim.startSimulation();

                // Final step: Print results when simulation is over
                List<Cloudlet> newList = broker.getCloudletReceivedList();

                //newList.addAll(globalBroker.getBroker().getCloudletReceivedList());

                CloudSim.stopSimulation();

                printCloudletList(newList);

                //creating excel result for average result
                String filename = new SimpleDateFormat("yyyyMMddHHmm'.csv'").format(new Date());
                PrintWriter pw = new PrintWriter(new File("FifoLoadBalancers_RunResultAverage_"+filename));
                StringBuilder sb = new StringBuilder();
                String line = "";

                for (int a=0; a<vmlist.size();a++){
                    line = "Average Response Time of Vm-" + vmlist.get(a).getId() + "   =  " + VmArt( newList, vmlist.get(a).getId());
                    sb.append(line);sb.append('\n');
                    Log.printLine(line);
                }

                for (int a=0; a<vmlist.size();a++){
                    line = "Last Exec Time of Vm-" + vmlist.get(a).getId() + "   =  " + VmMakespane( newList, vmlist.get(a).getId());
                    sb.append(line);sb.append('\n');
                    Log.printLine(line);
                }
                
                //Print the debt of each user to each datacenter
                //datacenter0.printDebts();

                Log.printLine("Simulation finished!");
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
 