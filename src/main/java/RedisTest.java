import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Random;

import static java.lang.System.in;
import static java.lang.System.out;

public class RedisTest {
    // Запуск докер-контейнера:
    // docker run --rm --name skill-redis -p 127.0.0.1:6379:6379/tcp -d redis

    // Для теста будем считать неактивными пользователей, которые не заходили 2 секунды
    private static final int DELETE_SECONDS_AGO = 2;

    // Допустим пользователи делают 500 запросов к сайту в секунду
    private static final int RPS = 500;

    // И всего на сайт заходило 1000 различных пользователей
    private static final int USERS = 1000;

    // Также мы добавим задержку между посещениями
    private static final int SLEEP = 1; // 1 миллисекунда

    private static final SimpleDateFormat DF = new SimpleDateFormat("HH:mm:ss");

    private static void log(int UsersOnline) {
        String log = String.format("[%s] Пользователей онлайн: %d", DF.format(new Date()), UsersOnline);
        out.println(log);
    }

    public static void main(String[] args) throws InterruptedException {

        RedisStorage redis = new RedisStorage();
        redis.init();

        // 20 пользователей
        for (int users = 0; users <= 20; users++) {
            redis.logPageVisitors(users);
        }
        int i = 0;
        int random = new Random().nextInt(9) + 1;

        for (; ; ) {
            //показ пользователей
            redis.printFirstVisitor();
            if (i == 10) {
                i = 0;
                random = new Random().nextInt(9) + 1;
            }
            i++;
            Thread.sleep(SLEEP);

            if (i == random) {
                redis.pageVisit(new Random().nextInt(10));
            }
        }


//
//
//        // Эмулируем 10 секунд работы сайта
//        for (int seconds = 0; seconds <= 10 ; seconds++) {
//
//            // Выполним 500 запросов
//            for (int request = 0; request <= RPS ; request++) {
//                int user_id = new Random().nextInt(USERS);
//                redis.logPageVisitors(user_id);
//                Thread.sleep(SLEEP);
//            }
//            redis.deleteOldEntries(DELETE_SECONDS_AGO);
//            int usersOnLine = redis.calculateUsersNumber();
//            log(usersOnLine);
//        }
//        redis.shutdown();
//    }

    }
}
