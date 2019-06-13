import org.apache.log4j.Logger;

/**
 * 模拟 日志 产生 log4j
 */
public class LoggerGenerator {

    private static Logger logger = Logger.getLogger(LoggerGenerator.class);

    public static void main(String[] args) {

        int index = 0;
        while (true){
            logger.info("value : " + index++);
            if (index == 100){
                index = 0;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

}
