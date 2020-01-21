package th.co.solar.solarapi;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class SolarapiApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	public void test(){
		Long result = 0L;
		Object object = 1L;
		if(object instanceof String){
			result = Long.valueOf((String)object);
		}else if(object instanceof Long){
			result = (Long)object;
		}
		System.out.println(result);

	}

}
