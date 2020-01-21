package th.co.solar.solarapi;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;

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

		int decimalPlaces = 0;
		BigDecimal bd = new BigDecimal("123456789.0423456890");

		bd = bd.setScale(decimalPlaces, BigDecimal.ROUND_HALF_UP);
		String string = bd.toString();
		System.out.println(string);
	}

}
