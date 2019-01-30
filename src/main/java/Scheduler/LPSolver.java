package Scheduler;

import Entity.Agent;
import Entity.Job;
import scpsolver.lpsolver.LinearProgramSolver;
import scpsolver.lpsolver.SolverFactory;
import scpsolver.problems.LPSolution;
import scpsolver.problems.LPWizard;
import scpsolver.problems.LPWizardConstraint;
import java.util.logging.Level;
import java.util.ArrayList;


public class LPSolver {

    /*public static void main(String[] args) {

        LPWizard lpw = new LPWizard();
        lpw.setMinProblem(true);
        lpw.plus("x1",5.0).plus("x2",10.0);
        lpw.addConstraint("c1",8,"<=").plus("x1",3.0).plus("x2",1.0);
        lpw.addConstraint("c2",4,"<=").plus("x2",4.0);
        LPWizardConstraint p1=lpw.addConstraint("c3", 2, ">=");
        p1.plus("x1",2.0);

        lpw.setAllVariablesInteger();
        LinearProgramSolver solver  = SolverFactory.newDefault();
        LPSolution lpsol =lpw.solve(solver);
        if(lpsol.getObjectiveValue()==0) {
            System.out.println("model is infeasible");
        } else {
            System.out.println(lpsol.toString());
        }
        ArrayList<Constraint> consList = new ArrayList<>();
        consList=lpw.getLP().getConstraints();
        for (int i=0;i<consList.size();i++)
        {
            System.out.println(consList.get(i).getName()+": RHS: "+consList.get(i).getRHS());
        }
    }*/

    /*public static void main(String[] args) {

        LPWizard lpw = new LPWizard();
        lpw.setMinProblem(true);
        lpw.plus("x1",5.0);
        lpw.plus("x2",10.0);
        LPWizardConstraint lpconsfirst=lpw.addConstraint("c1",8,"<=");
        lpconsfirst.plus("x1",3.0);
        lpconsfirst.plus("x2",1.0);

        lpw.addConstraint("c2",4,"<=").plus("x2",4.0);

        lpconsfirst=lpw.addConstraint("c3", 2, ">=");
        lpconsfirst.plus("x1",2.0);

        lpw.setAllVariablesInteger();
        LinearProgramSolver solver  = SolverFactory.newDefault();
        LPSolution lpsol =lpw.solve(solver);

        if(lpsol.getObjectiveValue()==0) {
            System.out.println("model is infeasible");
        } else {
            System.out.println(lpsol.toString());
        }
        ArrayList<Constraint> consList;
        consList=lpw.getLP().getConstraints();
        for (int i=0;i<consList.size();i++)
        {
            System.out.println(consList.get(i).getName()+": RHS: "+consList.get(i).getRHS());
        }
        System.out.println(lpw.getLP().getConstraints().size());
    }*/
    public static boolean placeExecutorILP(Job currentJob,Class classVar)
    {
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": in ILP Function");
        SchedulerUtil.placementTime=System.currentTimeMillis();
        ArrayList<Agent> placedAgents = new ArrayList<>();
        LPWizard lpw = new LPWizard();
        lpw.setMinProblem(true);

        //set objective function: agent selection as decision variables
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {
            lpw.plus("x-"+SchedulerUtil.agentList.get(i).getId(),SchedulerUtil.agentList.get(i).getPrice());
            lpw.setBoolean("x-"+SchedulerUtil.agentList.get(i).getId());
        }
        //totalCost constraint
        double totalCost=0;

        for(int i=0;i<SchedulerUtil.agentList.size();i++) {
            totalCost+=SchedulerUtil.agentList.get(i).getPrice();
        }
        LPWizardConstraint totalCostCons = lpw.addConstraint("tc_cons",totalCost,">=");
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {
            totalCostCons.plus("x-"+SchedulerUtil.agentList.get(i).getId(),SchedulerUtil.agentList.get(i).getPrice());
        }

        //set constraints: 1. executor placement constraint-> 1 executor in at most 1 agent
        for(int i=0;i<currentJob.getExecutors();i++) {

            LPWizardConstraint tmpsConsP = lpw.addConstraint("pc"+i,1,"=");

            for(int j=0;j<SchedulerUtil.agentList.size();j++) {
                tmpsConsP.plus("y-"+i+"-"+SchedulerUtil.agentList.get(j).getId(),1);
                lpw.setBoolean("y-"+i+"-"+SchedulerUtil.agentList.get(j).getId());
            }
        }

        //set constraints: 2. agent capacity constraint
        //cpu
        for(int j=0;j<SchedulerUtil.agentList.size();j++) {

            LPWizardConstraint tmpsConsCC = lpw.addConstraint("cc_cpu"+j,0,">=");

            for(int i=0;i<currentJob.getExecutors();i++) {
                tmpsConsCC.plus("y-"+i+"-"+SchedulerUtil.agentList.get(j).getId(),currentJob.getCoresPerExecutor());
            }

            tmpsConsCC.plus("x-"+SchedulerUtil.agentList.get(j).getId(),-SchedulerUtil.agentList.get(j).getCpu());
        }

        //memory
        for(int j=0;j<SchedulerUtil.agentList.size();j++) {

            LPWizardConstraint tmpsConsCM = lpw.addConstraint("cc_mem"+j,0,">=");

            for(int i=0;i<currentJob.getExecutors();i++) {
                tmpsConsCM.plus("y-"+i+"-"+SchedulerUtil.agentList.get(j).getId(),Math.ceil(currentJob.getTotalExecutorMemory()));
            }
            tmpsConsCM.plus("x-"+SchedulerUtil.agentList.get(j).getId(),-SchedulerUtil.agentList.get(j).getMem());

        }

        //bound constraints
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {
            lpw.addConstraint("bc0-x-"+SchedulerUtil.agentList.get(i).getId(), 0, "<=").plus("x-"+SchedulerUtil.agentList.get(i).getId(),1);
            lpw.addConstraint("bc1-x-"+SchedulerUtil.agentList.get(i).getId(), 1, ">=").plus("x-"+SchedulerUtil.agentList.get(i).getId(),1);

        }
        for(int i=0;i<currentJob.getExecutors();i++) {
            for(int j=0;j<SchedulerUtil.agentList.size();j++) {
                lpw.addConstraint("bc0-y-"+i+"-"+SchedulerUtil.agentList.get(j).getId(), 0, "<=").plus("y-"+i+"-"+SchedulerUtil.agentList.get(j).getId(),1);
                lpw.addConstraint("bc1-y-"+i+"-"+SchedulerUtil.agentList.get(j).getId(), 1, ">=").plus("y-"+i+"-"+SchedulerUtil.agentList.get(j).getId(),1);
            }
        }

        //already turned-on machine constraints
        /* ************************** */
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {
            if(SchedulerUtil.agentList.get(i).isUsed()) {
                lpw.addConstraint("active-x-"+SchedulerUtil.agentList.get(i).getId(), 1, "=").plus("x-"+SchedulerUtil.agentList.get(i).getId(),1);
            }
        }
        /*Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Total Constraints: "+lpw.getLP().getConstraints().size());
        for (int i=0;i< lpw.getLP().getConstraints().size();i++) {
            Log.SchedulerLogging.log(Level.INFO,"constraint-"+i+": "+lpw.getLP().getConstraints().get(i).getName());
        }*/
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Solving LP");
        LinearProgramSolver solver  = SolverFactory.newDefault();
        LPSolution lpsol =lpw.solve(solver);

        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Finished solving LP. Objective Value: "+lpsol.getObjectiveValue());
        Log.SchedulerLogging.log(Level.INFO,LPSolver.class.getName() +"\n"+ lpsol.toString());

        for(int i=0;i<SchedulerUtil.agentList.size();i++) {
            Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() +": " +SchedulerUtil.agentList.get(i).getId()+"-> CPU-"+SchedulerUtil.agentList.get(i).getCpu()+" MEM-"+SchedulerUtil.agentList.get(i).getMem());
        }
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": current job->coresPerExec: "+currentJob.getCoresPerExecutor()+" memPerExec: "+currentJob.getMemPerExecutor()+" E: "+currentJob.getExecutors());
        double objVal=lpsol.getObjectiveValue();
        //objVal=Math.floor(objVal * 100 + 0.5) / 100;
        if((objVal == Math.floor(objVal)) && !Double.isInfinite(objVal)&&objVal>0&&objVal<=totalCost) {
           // Log.SchedulerLogging.log(Level.INFO,LPSolver.class.getName() + lpsol.toString());
            for (int i = 0; i < currentJob.getExecutors(); i++) {
                for (int j = 0; j < SchedulerUtil.agentList.size(); j++) {
                    if (lpsol.getBoolean("y-"+i+"-"+SchedulerUtil.agentList.get(j).getId())) {
                        if (SchedulerUtil.agentList.get(j).getCpu() >= currentJob.getCoresPerExecutor() &&
                                SchedulerUtil.agentList.get(j).getMem() >= Math.ceil(currentJob.getTotalExecutorMemory())) {

                            SchedulerUtil.agentList.get(j).setCpu(SchedulerUtil.agentList.get(j).getCpu() - currentJob.getCoresPerExecutor());
                            SchedulerUtil.agentList.get(j).setMem(SchedulerUtil.agentList.get(j).getMem() - Math.ceil(currentJob.getTotalExecutorMemory()));
                            placedAgents.add(SchedulerUtil.agentList.get(j));
                            Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + "Added Agent " + placedAgents.get(placedAgents.size() - 1).getId());
                            break;
                        }
                    }
                }
            }



            if(placedAgents.size()<currentJob.getExecutors()) {
                Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Model is infeasible to solve, returning failure, no exec placed");
                for(int i=0;i<placedAgents.size();i++){
                    placedAgents.get(i).setCpu(placedAgents.get(i).getCpu() + currentJob.getCoresPerExecutor());
                    placedAgents.get(i).setMem(placedAgents.get(i).getMem() + Math.ceil(currentJob.getTotalExecutorMemory()));
                }
                return false;
            }
            SchedulerUtil.placementTime=System.currentTimeMillis()-SchedulerUtil.placementTime;
            //if success
            currentJob.setAllocatedExecutors(currentJob.getExecutors());
            SchedulerUtil.resourceReservation(placedAgents, currentJob, classVar);
            //calculate time //TODO

            return true;
        }
        else {
            Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Model is infeasible to solve, returning failure, obj not integer");
            return false;
        }
    }
}