 
package com.paypal.juno.transport.socket;

import com.paypal.juno.client.JunoClientConfigHolder;
import com.paypal.juno.conf.JunoPropertiesProvider;
import com.paypal.juno.conf.JunoPropertiesProviderTest;
import com.paypal.juno.exception.JunoException;
import java.net.InetSocketAddress;
import java.net.URL;
import org.junit.Test;
import org.testng.AssertJUnit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SocketConfigHolderTest{
	URL url = JunoPropertiesProviderTest.class.getClassLoader().getResource("juno.properties");
	JunoPropertiesProvider jpp = new JunoPropertiesProvider(url);

	
	@Test
	public void TestSocketConfigHolder(){
		try{
		JunoClientConfigHolder jch = new JunoClientConfigHolder(jpp);
		SocketConfigHolder sch = new SocketConfigHolder(jch);
		assertEquals(sch.getConnectionLifeTime(),5000);
		InetSocketAddress addr = sch.getInetAddress();
		assertNotNull(addr);
		assertEquals(sch.getConnectTimeout(),1000);
		}catch(JunoException e){
			AssertJUnit.assertTrue ("Exception :"+e.getMessage(), false);
		}
	}
}