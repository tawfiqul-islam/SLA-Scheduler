package Operator;

import Entity.Agent;
import Entity.Framework;
import Settings.Settings;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import java.util.logging.Level;
import org.json.*;
import java.io.*;
import java.util.*;

public class HTTPAPI {

    public static ServerResponse RESERVE(String role, double CPU, double memory, String agentID)
    {
        JSONArray reserveJSONObj = constructReservationJSON(role,CPU,memory);
        return reservationHTTPPost(reserveJSONObj,agentID,Settings.mesosMasterURI+"/master/reserve");
    }

    public static ServerResponse UNRESERVE(String role, double CPU, double memory, String agentID)
    {
        JSONArray reserveJSONObj = constructReservationJSON(role,CPU,memory);
        return reservationHTTPPost(reserveJSONObj,agentID,Settings.mesosMasterURI+"/master/unreserve");
    }

    public static boolean GET_HEALTH()
    {
        JSONObject statusJSONObj = constructStatusJSON(Constants.GET_HEALTH);
        ServerResponse serverResponseObj = statusHTTPPOST(statusJSONObj,Settings.mesosMasterURI+"/api/v1");
        return parseGetHealthResponse(serverResponseObj.getResponseString());
    }

    public static ArrayList<Agent> GET_AGENT()
    {
        JSONObject statusJSONObj = constructStatusJSON(Constants.GET_STATE);
        ServerResponse serverResponseObj = statusHTTPPOST(statusJSONObj,Settings.mesosMasterURI+"/api/v1");
        //System.out.println("GET agent response: "+serverResponseObj.responseString);
        return parseAgents(serverResponseObj.getResponseString());
    }

    public static ArrayList<Framework> GET_FRAMEWORK()
    {
        JSONObject statusJSONObj = constructStatusJSON(Constants.GET_STATE);
        ServerResponse serverResponseObj = statusHTTPPOST(statusJSONObj,Settings.mesosMasterURI+"/api/v1");
        //System.out.println("Get framework response: "+serverResponseObj.responseString);
        return parseFrameworks(serverResponseObj.getResponseString());
    }

    private static ServerResponse reservationHTTPPost(JSONArray obj, String agentId, String httpURI) {

        String outputStr = "";
        ServerResponse serverResponseObj = new ServerResponse();

        try {

            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpPost post = new HttpPost(httpURI);
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("slaveId", agentId));
            params.add(new BasicNameValuePair("resources", obj.toString()));
            CloseableHttpResponse response = null;

            try {
                post.setEntity(new UrlEncodedFormEntity(params));
                post.setHeader("Content-Type", "application/x-www-form-urlencoded;charset=utf-8");
                post.setHeader("Accept", "application/json");
                response = httpClient.execute(post);
                //System.out.println(response.getStatusLine());
                serverResponseObj.setStatusCode(response.getStatusLine().getStatusCode());
                BufferedReader br = new BufferedReader(
                        new InputStreamReader((response.getEntity().getContent())));

                String tmpStr;
                while ((tmpStr = br.readLine()) != null) {
                    outputStr += tmpStr;
                }

            } catch(Exception e){
                Log.SchedulerLogging.log(Level.SEVERE,HTTPAPI.class.getName()+" Exception in reservationHTTPPost: "+ e.toString());
            }finally {
                response.close();
            }

        }catch(Exception e) {
            Log.SchedulerLogging.log(Level.SEVERE,HTTPAPI.class.getName()+" Exception in reservationHTTPPost: "+ e.toString());
        }

        serverResponseObj.setResponseString(outputStr);

        return serverResponseObj;
    }

    private static ServerResponse statusHTTPPOST(JSONObject obj, String httpURI) {

        String outputStr = "";
        ServerResponse serverResponseObj = new ServerResponse();

        try {

            CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost postRequest = new HttpPost(httpURI);

            StringEntity input = new StringEntity(obj.toString());

            postRequest.setEntity(input);
            postRequest.setHeader("Content-Type", "application/json");
            postRequest.setHeader("Accept", "application/json");

            CloseableHttpResponse response = httpClient.execute(postRequest);

            //System.out.println(response.getStatusLine());
            serverResponseObj.setStatusCode(response.getStatusLine().getStatusCode());

            BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

            String tmpStr;
            while ((tmpStr = br.readLine()) != null) {
                outputStr += tmpStr;
            }

            httpClient.close();

        } catch (Exception e) {
            Log.SchedulerLogging.log(Level.SEVERE,HTTPAPI.class.getName()+" Exception in statusHTTPPost: "+ e.toString());
        }

        serverResponseObj.setResponseString(outputStr);
        return serverResponseObj;
    }

    private static JSONArray constructReservationJSON(String role, Double cpu, Double mem) {

        JSONArray resourceArray = new JSONArray();
        JSONObject cpuObj = new JSONObject();
        JSONObject memObj = new JSONObject();
        try {

            cpuObj.put("type","SCALAR");
            cpuObj.put("name","cpus");
            cpuObj.put("reservation",new JSONObject());
            cpuObj.put("role",role);
            cpuObj.put("scalar",new JSONObject().put("value",cpu));

            memObj.put("type","SCALAR");
            memObj.put("name","mem");
            memObj.put("reservation",new JSONObject());
            memObj.put("role",role);
            memObj.put("scalar",new JSONObject().put("value",mem));

            resourceArray.put(cpuObj);
            resourceArray.put(memObj);

        } catch (Exception e) {
            Log.SchedulerLogging.log(Level.SEVERE,HTTPAPI.class.getName()+" Exception in constructReservationJSON: "+ e.toString());
        }

        return resourceArray;
    }

    private static JSONObject constructStatusJSON(int msgType) {

        JSONObject obj = new JSONObject();

        try {
            switch(msgType) {
                case Constants.GET_HEALTH:
                    obj.put("type", "GET_HEALTH");
                    break;
                case Constants.GET_STATE:
                    obj.put("type", "GET_STATE");
                    break;
                default:
                    break;
            }

        } catch (JSONException e) {
            Log.SchedulerLogging.log(Level.SEVERE,HTTPAPI.class.getName()+" Exception in constructStatusJSON: "+ e.toString());
        }

        return obj;
    }

    private static boolean parseGetHealthResponse(String responseStr){

        Boolean health = null;
        try {
            health = (Boolean) new JSONObject(responseStr).getJSONObject("get_health").get("healthy");
        } catch (JSONException e) {
            Log.SchedulerLogging.log(Level.SEVERE,HTTPAPI.class.getName()+" Exception in parseGetHealthResponse: "+ e.toString());
        }
        //System.out.println(health);
        return health;
    }

    private static ArrayList<Agent> parseAgents(String responseStr) {

        ArrayList<Agent> agentList= new ArrayList<>();
        JSONArray agents;

        try {
            agents = (JSONArray) new JSONObject(responseStr).getJSONObject("get_state").getJSONObject("get_agents").get("agents");

            for (int i = 0; i < agents.length(); i++) {

                Agent agentObj = new Agent();
                agentObj.setActive(agents.getJSONObject(i).getBoolean("active"));
                agentObj.setId(agents.getJSONObject(i).getJSONObject("agent_info").getJSONObject("id").getString("value"));

                JSONArray resources = agents.getJSONObject(i).getJSONObject("agent_info").getJSONArray("resources");
                for (int j = 0; j < resources.length(); j++) {
                    String resourceName = resources.getJSONObject(j).getString("name");

                    switch (resourceName) {
                        case "cpus":
                            agentObj.setCpu(resources.getJSONObject(j).getJSONObject("scalar").getDouble("value"));
                            agentObj.setDefaultCPU(agentObj.getCpu());
                            break;
                        case "mem":
                            agentObj.setMem(resources.getJSONObject(j).getJSONObject("scalar").getDouble("value"));
                            agentObj.setDefaultMEM(agentObj.getMem());
                            break;
                        case "disk":
                            agentObj.setDisk(resources.getJSONObject(j).getJSONObject("scalar").getDouble("value"));
                            break;
                        case "ports":
                            JSONObject ports = resources.getJSONObject(j).getJSONObject("ranges").getJSONArray("range").getJSONObject(0);
                            agentObj.setPortStart(ports.getInt("begin"));
                            agentObj.setPortEnd(ports.getInt("end"));
                            break;
                        default:
                            break;
                    }
                }
                agentObj.setRegisteredTime(agents.getJSONObject(i).getJSONObject("registered_time").getLong("nanoseconds"));
                agentList.add(agentObj);
            }

        } catch (JSONException e) {
            Log.SchedulerLogging.log(Level.SEVERE,HTTPAPI.class.getName()+" Exception in parseAgents: "+ e.toString());
        }
        return agentList;
    }

    private static ArrayList<Framework> parseFrameworks(String responseStr) {

        ArrayList<Framework> frameworkList = new ArrayList<Framework>();
        JSONArray frameworks;
        try {
            JSONObject tmp = new JSONObject(responseStr);
            if (tmp.getJSONObject("get_state").getJSONObject("get_frameworks").has("completed_frameworks")) {

                frameworks = (JSONArray) new JSONObject(responseStr).getJSONObject("get_state").getJSONObject("get_frameworks").get("completed_frameworks");

                for (int i = 0; i < frameworks.length(); i++) {
                    Framework frameworkObj = new Framework();

                    //framework is finished or active
                    frameworkObj.setActive(frameworks.getJSONObject(i).getBoolean("active"));
                    //framework start time
                    frameworkObj.setStartTime(frameworks.getJSONObject(i).getJSONObject("registered_time").getLong("nanoseconds"));
                    //framework finish time
                    frameworkObj.setFinishTime(frameworks.getJSONObject(i).getJSONObject("unregistered_time").getLong("nanoseconds"));

                    //framework id
                    frameworkObj.setID(frameworks.getJSONObject(i).getJSONObject("framework_info").getJSONObject("id").getString("value"));
                    //framework role
                    frameworkObj.setRole(frameworks.getJSONObject(i).getJSONObject("framework_info").getString("role"));

                    System.out.println(frameworks.getJSONObject(i));
                    //JSONArray tmpArray = (JSONArray)frameworks.getJSONObject(i).get("completed_tasks");
                    //frameworkObj.setExecutors(tmpArray.length());

                    frameworkList.add(frameworkObj);
                }
            }
        } catch(JSONException e){
            Log.SchedulerLogging.log(Level.SEVERE,HTTPAPI.class.getName()+" Exception in parseFrameworks: "+ e.toString());
        }
        return frameworkList;
    }
}