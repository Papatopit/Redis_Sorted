import org.redisson.Redisson;
import org.redisson.api.RKeys;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.client.RedisConnectionException;

import java.util.Date;
import static java.lang.System.out;

public class RedisStorage {

    // Объект для работы с Redis
    private RedissonClient redisson;

    // Объект для работы с ключами
    private RKeys rKeys;

    // Объект для работы с Sorted Set'ом
    private RScoredSortedSet<String> onLineUsers;

    private final static String KEY = "ONLINE_USERS";

    private double getTs(){
        return new Date().getTime()/1000;
    }

    // Пример вывода всех ключей
    public void listKeys() {
        Iterable<String> keys = rKeys.getKeys();
        for (String key: keys){
            out.println("KEY: " + key + ", type:" + rKeys.getType(key));
        }
    }

    void init(){
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        try {
            redisson = Redisson.create(config);
        }catch (RedisConnectionException Exc){
            out.println("Не удалось подключиться к Redis");
            out.println(Exc.getMessage());
        }
        rKeys = redisson.getKeys();
        onLineUsers = redisson.getScoredSortedSet(KEY);
        rKeys.delete(KEY);
    }

    void shutdown(){
        redisson.shutdown();
    }

    // Фиксирует посещение пользователем страницы
    void logPageVisitors(int user_id){

        //ZADD ONLINE_USERS
        onLineUsers.add(getTs(),String.valueOf(user_id));
    }

    // Удаляет
    void deleteOldEntries(int secondAgo){

        //ZREVRANGEBYSCORE ONLINE_USERS 0 <time_5_seconds_ago>
        onLineUsers.removeRangeByScore(0,true,getTs() - secondAgo,true);
    }

    int calculateUsersNumber(){

        //ZCOUNT ONLINE_USERS
       return onLineUsers.count(Double.NEGATIVE_INFINITY,true,Double.POSITIVE_INFINITY,true);
    }

    void pageVisit(Integer user_id){

        //ZADD ONLINE_USERS
        out.println("Пользователь " + user_id + " оплатил платную услугу");
        onLineUsers.remove(user_id);
        onLineUsers.add(onLineUsers.firstScore() - 1, String.valueOf(user_id));
    }

    void printFirstVisitor(){
        out.println("Пользователь "+ onLineUsers.first());
        Integer name = Integer.valueOf(onLineUsers.first());
        onLineUsers.remove(onLineUsers.first());
        onLineUsers.add(getTs(),String.valueOf(name));
    }


}


