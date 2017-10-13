package Operator;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.json.*;
import java.io.*;

public class MessageEncoder {

    public static void main(String[] args) {

       JSONObject healthObj = get_health();
       ServerResponse serverResponseObj = httpPOST(healthObj);
       get_health_response(serverResponseObj.responseString);
       JSONObject reserveObj = reserve_resources("04425ed8-3917-4861-8e38-9ced3e8a1b32-S0","r1",1.0,3.5);
       ServerResponse serverResponseObj2 = httpPOST(reserveObj);
       System.out.println(serverResponseObj2.statusCode);
    }

    private static ServerResponse httpPOST(JSONObject obj) {

        String outputStr = "";
        ServerResponse serverResponseObj = new ServerResponse();
        try {

            CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost postRequest = new HttpPost(
                    "http://127.0.0.1:5050/api/v1");

            StringEntity input = new StringEntity(obj.toString());
            input.setContentType("application/json");
            postRequest.setEntity(input);
            postRequest.setHeader("Content-Type", "application/json");
            postRequest.setHeader("Accept", "application/json");

            CloseableHttpResponse response = httpClient.execute(postRequest);

            serverResponseObj.statusCode=response.getStatusLine().getStatusCode();
            /*
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + response.getStatusLine().getStatusCode());
            }
*/
            //System.out.println(response.getEntity().getContentType());
            BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

            String tmpStr;
            while ((tmpStr = br.readLine()) != null) {
                outputStr += tmpStr;
            }

            httpClient.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

        serverResponseObj.responseString=outputStr;
        return serverResponseObj;
    }

    private static JSONObject get_health() {
        JSONObject obj = new JSONObject();
        try {
            obj.put("type", "GET_HEALTH");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        System.out.println(obj);
        return obj;
    }
    private static void get_health_response(String responseStr){
        Boolean health = null;
        try {
            health = (Boolean) new JSONObject(responseStr).getJSONObject("get_health").get("healthy");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        System.out.println(health);
    }
    private static JSONObject reserve_resources(String agentID, String role, Double cpu, Double mem) {
        JSONObject obj = new JSONObject();
        JSONObject cpuObj = new JSONObject();
        JSONObject memObj = new JSONObject();
        try {
            obj.put("type", "RESERVE_RESOURCES");
            obj.put("reserve_resources",new JSONObject().put("agent_id",new JSONObject().put("value",agentID)));


            cpuObj.put("type","SCALAR");
            cpuObj.put("name","cpus");
            cpuObj.put("reservation","");
            cpuObj.put("role",role);
            cpuObj.put("scalar",new JSONObject().put("value",cpu));

            memObj.put("type","SCALAR");
            memObj.put("name","mem");
            memObj.put("reservation","");
            memObj.put("role",role);
            memObj.put("scalar",new JSONObject().put("value",mem));

            JSONArray resourceArray = new JSONArray();

            resourceArray.put(cpuObj);
            resourceArray.put(memObj);

            obj.put("resources",resourceArray);


        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(obj);
        return obj;
    }

    private static JSONObject unreserve_resources(String agentID, String role, Double cpu, Double mem) {
        JSONObject obj = new JSONObject();
        JSONObject cpuObj = new JSONObject();
        JSONObject memObj = new JSONObject();
        try {
            obj.put("type", "UNRESERVE_RESOURCES");
            obj.put("unreserve_resources",new JSONObject().put("agent_id",new JSONObject().put("value",agentID)));


            cpuObj.put("type","SCALAR");
            cpuObj.put("name","cpus");
            cpuObj.put("reservation","");
            cpuObj.put("role",role);
            cpuObj.put("scalar",new JSONObject().put("value",cpu));

            memObj.put("type","SCALAR");
            memObj.put("name","mem");
            memObj.put("reservation","");
            memObj.put("role",role);
            memObj.put("scalar",new JSONObject().put("value",mem));

            JSONArray resourceArray = new JSONArray();

            resourceArray.put(cpuObj);
            resourceArray.put(memObj);

            obj.put("resources",resourceArray);


        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(obj);
        return obj;
    }

}