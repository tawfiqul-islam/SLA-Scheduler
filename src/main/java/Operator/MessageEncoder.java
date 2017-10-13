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


        JSONObject obj = get_health();
        httpPOST(obj);
    }

    public static JSONObject get_health() {
        JSONObject obj = new JSONObject();
        try {
            obj.put("type", "GET_HEALTH");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        System.out.println(obj);
        return obj;
    }

    public static void httpPOST(JSONObject obj) {

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
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "
                        + response.getStatusLine().getStatusCode());
            }

            //System.out.println(response.getEntity().getContentType());
            BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

            String output = "";
            String str;
            while ((str = br.readLine()) != null) {
                output += str;
            }

            httpClient.close();
            System.out.println("Output from Server .... ");
            Boolean health = (Boolean) new JSONObject(output).getJSONObject("get_health").get("healthy");
            System.out.println(health);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}