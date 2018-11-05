package Scheduler;

import Entity.Agent;
import Entity.Job;
import scpsolver.lpsolver.LinearProgramSolver;
import scpsolver.lpsolver.SolverFactory;
import scpsolver.problems.LPSolution;
import scpsolver.problems.LPWizard;
import scpsolver.problems.LPWizardConstraint;

import java.util.ArrayList;


public class LPSolver {

    public static void main(String[] args) {

        LPWizard lpw = new LPWizard();
        lpw.plus("x1",5.0).plus("x2",10.0);
        lpw.addConstraint("c1",8,"<=").plus("x1",3.0).plus("x2",1.0);
        lpw.addConstraint("c2",4,"<=").plus("x2",4.0);
        LPWizardConstraint p1=lpw.addConstraint("c3", 2, ">=");
        p1.plus("x1",2.0);
        lpw.setMinProblem(true);
        lpw.setAllVariablesInteger();
        lpw.setBoolean("x2");
        LinearProgramSolver solver  = SolverFactory.newDefault();
        LPSolution lpsol =lpw.solve(solver);
        if(lpsol.getObjectiveValue()==0) {
            System.out.println("model is infeasible");
        } else {
            System.out.println(lpsol.toString());
        }
    }
    public static boolean placeExecutorILP(Job currentJob,Class classVar)
    {
        ArrayList<Agent> placedAgents = new ArrayList<>();
        LPWizard lpw = new LPWizard();
        lpw.setMinProblem(true);

        //set objective function: agent selection as decision variables
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {
            lpw.plus(SchedulerUtil.agentList.get(i).getId(),SchedulerUtil.agentList.get(i).getPrice());
            lpw.setBoolean(SchedulerUtil.agentList.get(i).getId());
        }

        //set constraints: 1. executor placement constraint-> 1 executor in at most 1 agent

        for(int i=0;i<currentJob.getExecutors();i++) {

            LPWizardConstraint tmpsConsP = lpw.addConstraint("pc"+i,1,"=");

            for(int j=0;j<SchedulerUtil.agentList.size();j++) {
                tmpsConsP.plus(i+SchedulerUtil.agentList.get(j).getId(),1);
            }

            tmpsConsP.setAllVariablesBoolean();
        }

        //set constraints: 2. agent capacity constraint

        //cpu
        for(int i=0;i<SchedulerUtil.agentList.size();i++) {

            LPWizardConstraint tmpsConsCC = lpw.addConstraint("cc_cpu"+i,0,">=");

            for(int j=0;j<currentJob.getExecutors();j++) {
                tmpsConsCC.plus(j+SchedulerUtil.agentList.get(i).getId(),(int)(SchedulerUtil.agentList.get(i).getCpu()/currentJob.getCoresPerExecutor()));
            }
            tmpsConsCC.plus(SchedulerUtil.agentList.get(i).getId(),-SchedulerUtil.agentList.get(i).getCpu());
        }

        //memory

        for(int i=0;i<SchedulerUtil.agentList.size();i++) {

            LPWizardConstraint tmpsConsCM = lpw.addConstraint("cc_mem"+i,0,">=");

            for(int j=0;j<currentJob.getExecutors();j++) {
                tmpsConsCM.plus(j+SchedulerUtil.agentList.get(i).getId(),(int)(SchedulerUtil.agentList.get(i).getMem()/Math.ceil(currentJob.getTotalExecutorMemory())));
            }
            tmpsConsCM.plus(SchedulerUtil.agentList.get(i).getId(),-SchedulerUtil.agentList.get(i).getMem());
        }

        LinearProgramSolver solver  = SolverFactory.newDefault();
        LPSolution lpsol =lpw.solve(solver);

        if(lpsol.getObjectiveValue()!=0) {
            for (int i = 0; i < currentJob.getExecutors(); i++) {
                for (int j = 0; j < SchedulerUtil.agentList.size(); j++) {
                    if (lpsol.getBoolean(i + SchedulerUtil.agentList.get(j).getId())) {
                        placedAgents.add(SchedulerUtil.agentList.get(j));
                        break;
                    }
                }
            }

            for (int i = 0; i < placedAgents.size(); i++) {
                placedAgents.get(i).setCpu(SchedulerUtil.agentList.get(i).getCpu() - currentJob.getCoresPerExecutor());
                placedAgents.get(i).setMem(SchedulerUtil.agentList.get(i).getMem() - Math.ceil(currentJob.getTotalExecutorMemory()));
            }
            //if success
            currentJob.setAllocatedExecutors(currentJob.getExecutors());
            SchedulerUtil.resourceReservation(placedAgents, currentJob, classVar);
            //calculate time //TODO

            return true;
        }
        return false;
    }
}