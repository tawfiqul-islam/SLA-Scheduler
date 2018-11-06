package Scheduler;

import Entity.Agent;
import Entity.Job;
import scpsolver.lpsolver.LinearProgramSolver;
import scpsolver.lpsolver.SolverFactory;
import scpsolver.problems.LPSolution;
import scpsolver.problems.LPWizard;
import scpsolver.problems.LPWizardConstraint;
import scpsolver.constraints.*;

import java.util.logging.Level;

import java.util.ArrayList;


public class LPSolver {

    public static void main(String[] args) {

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
    }
    public static boolean placeExecutorILP(Job currentJob,Class classVar)
    {
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": in ILP Function");
        SchedulerUtil.placementTime=System.currentTimeMillis();
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": recorded current system time");
        ArrayList<Agent> placedAgents = new ArrayList<>();
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": declared Arraylist");
        LPWizard lpw = new LPWizard();
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": declared lpwizard");
        lpw.setMinProblem(true);
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": set min prob");

        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Setting Objective Function");
        //set objective function: agent selection as decision variables
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {
            lpw.plus(SchedulerUtil.agentList.get(i).getId(),SchedulerUtil.agentList.get(i).getPrice());
            lpw.setBoolean(SchedulerUtil.agentList.get(i).getId());
        }
        //set constraints: 1. executor placement constraint-> 1 executor in at most 1 agent
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Setting Executor Placement Constraints");
        for(int i=0;i<currentJob.getExecutors();i++) {

            LPWizardConstraint tmpsConsP = lpw.addConstraint("pc"+i,1,"=");

            for(int j=0;j<SchedulerUtil.agentList.size();j++) {
                tmpsConsP.plus(i+SchedulerUtil.agentList.get(j).getId(),1);
            }

            tmpsConsP.setAllVariablesBoolean();
        }

        //set constraints: 2. agent capacity constraint
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Setting Agent CPU capacity Constraints");
        //cpu
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {

            LPWizardConstraint tmpsConsCC = lpw.addConstraint("cc_cpu"+i,0,">=");

            for(int j=0;j<currentJob.getExecutors();j++) {
                tmpsConsCC.plus(j+SchedulerUtil.agentList.get(i).getId(),(int)(SchedulerUtil.agentList.get(i).getCpu()/currentJob.getCoresPerExecutor()));
            }
            tmpsConsCC.plus(SchedulerUtil.agentList.get(i).getId(),-(int)SchedulerUtil.agentList.get(i).getCpu());
        }

        //memory
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Setting Agent Memory capacity Constraints");
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {

            LPWizardConstraint tmpsConsCM = lpw.addConstraint("cc_mem"+i,0,">=");

            for(int j=0;j<currentJob.getExecutors();j++) {
                tmpsConsCM.plus(j+SchedulerUtil.agentList.get(i).getId(),(int)(SchedulerUtil.agentList.get(i).getMem()/Math.ceil(currentJob.getTotalExecutorMemory())));
            }
            tmpsConsCM.plus(SchedulerUtil.agentList.get(i).getId(),-(int)SchedulerUtil.agentList.get(i).getMem());
        }
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Solving LP");
        LinearProgramSolver solver  = SolverFactory.newDefault();
        LPSolution lpsol =lpw.solve(solver);
        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Finished solving LP. Objective Value: "+lpsol.getObjectiveValue());

        if(lpsol.getObjectiveValue()!=0) {
            for (int i = 0; i < currentJob.getExecutors(); i++) {
                for (int j = 0; j < SchedulerUtil.agentList.size(); j++) {
                    if (lpsol.getBoolean(i + SchedulerUtil.agentList.get(j).getId())) {
                        placedAgents.add(SchedulerUtil.agentList.get(j));
                        Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + "Added Agent "+placedAgents.get(placedAgents.size()-1).getId());
                        break;
                    }
                }
            }

            for (int i = 0; i < placedAgents.size(); i++) {
                placedAgents.get(i).setCpu(SchedulerUtil.agentList.get(i).getCpu() - currentJob.getCoresPerExecutor());
                placedAgents.get(i).setMem(SchedulerUtil.agentList.get(i).getMem() - Math.ceil(currentJob.getTotalExecutorMemory()));
            }

            SchedulerUtil.placementTime=System.currentTimeMillis()-SchedulerUtil.placementTime;
            //if success
            currentJob.setAllocatedExecutors(currentJob.getExecutors());
            SchedulerUtil.resourceReservation(placedAgents, currentJob, classVar);
            //calculate time //TODO

            return true;
        }
        else {
            Log.SchedulerLogging.log(Level.INFO, LPSolver.class.getName() + ": Model is infeasible to solve, returning failure");
            return false;
        }

    }
}