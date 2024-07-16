import java.sql.*;

public class JDBCRunner {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";        
    private static final String DATABASE_NAME = "game_roguelike";         

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  
    public static final String DATABASE_PASS = "postgres";              

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            getBosses(connection);
            System.out.println();
            getCharacters(connection);
            System.out.println();
            getItems(connection);
            System.out.println();

            getBossesFromTwoToThreeFloors(connection);
            System.out.println();
            getMobsFromTwoToFourFloors(connection);
            System.out.println();
            getCharactersDamageTwo(connection);
            System.out.println();


            System.out.println();
            removeBossesWhereHealthEight(connection);
            System.out.println();
            removeCharactersWhereHealthTen(connection);
            System.out.println();
            correctCharacter(connection, "Каин", 3);
            System.out.println();
            addCharacter(connection, 5, "Лазарь", 3, 9);
            System.out.println();


        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            throw new RuntimeException(e);
        }
    }

    // region // Проверка окружения и доступа к базе данных

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    // endregion

    // region // SELECT-запросы без параметров в одной таблице

    private static void getBosses(Connection connection) throws SQLException {
        // имена столбцов
        String columnName0 = "boss_id", columnName1 = "boss_name", columnName2 = "boss_health", columnName3 = "boss_damage", columnName4 = "boss_floor";
        // значения ячеек
        int param0 = -1, param2 = -1, param3 = -1, param4 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM bosses;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param4 = rs.getInt(columnName4);
            param3 = rs.getInt(columnName3);
            param2 = rs.getInt(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4);
        }
    }

    static void getCharacters(Connection connection) throws SQLException {

        String columnName0 = "character_id", columnName1 = "character_name", columnName2 = "character_damage", columnName3 = "character_health";

        // значения ячеек
        int param0 = -1, param2 = -1, param3 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM characters;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(columnName0); 
            param1 = rs.getString(columnName1);
            param2 = rs.getInt(columnName2);
            param3 = rs.getInt(columnName3);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3);
        }
    }

    static void getItems(Connection connection) throws SQLException {
        String columnName0 = "item_id", columnName1 = "item_name", columnName2 = "item_floor", columnName3 = "stats_up", columnName4 = "stats_down";

        // значения ячеек
        int param0 = -1, param2 = -1;
        String param1 = null, param3 = null, param4 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM items;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(columnName0); 
            param1 = rs.getString(columnName1);
            param2 = rs.getInt(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getString(columnName4);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4);
        }
    }

    static void getBossesFromTwoToThreeFloors(Connection connection) throws SQLException {

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT boss_id, boss_name, boss_health, boss_damage, boss_floor " +
                        "FROM bosses " +
                        "WHERE boss_floor >= 2 and boss_floor <= 3;");  
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3) + " | " + rs.getInt(4) + " | " + rs.getInt(5));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }


    // endregion

    // region // SELECT-запросы с параметрами и объединением таблиц


    static void getMobsFromTwoToFourFloors(Connection connection) throws SQLException {

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT mob_id, mob_name, mob_damage, mob_health, mob_floor " +
                        "FROM mobs " +
                        "WHERE mob_floor >= 2 and mob_floor <= 4;");  
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3) + " | " + rs.getInt(4) + " | " + rs.getInt(5));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    static void getCharactersDamageTwo(Connection connection) throws SQLException {

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT character_id, character_name, character_damage, character_health " +
                        "FROM characters " +
                        "WHERE character_damage = 2;");  
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getInt(3) + " | " + rs.getInt(4));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    // endregion

    // region // CUD-запросы на добавление, изменение и удаление записей

    private static void addCharacter(Connection connection, int character_id, String name, int damage, int health) throws SQLException {
        if (name == null || name.isBlank() || damage <= 0 || health <= 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO characters(character_id, character_name, character_damage, character_health) VALUES (?, ?, ?, ?) returning character_id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, character_id);
        statement.setString(2, name);  
        statement.setInt(3, damage);  
        statement.setInt(4, health);

        int count =
                statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор персонажа " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " character");
        getCharacters(connection);
    }

    private static void correctCharacter(Connection connection, String character_name, int character_damage) throws SQLException {
        if (character_name == null || character_name.isBlank() || character_damage <= 0)
            return;

        PreparedStatement statement = connection.prepareStatement("UPDATE characters SET character_damage=? WHERE character_name=?;");
        statement.setInt(1, character_damage); // сначала что передаем
        statement.setString(2, character_name);   // затем по чему ищем

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        System.out.println("UPDATEd " + count + " characters");
        getCharacters(connection);
    }

    private static void removeCharactersWhereHealthTen(Connection connection) throws SQLException {

        PreparedStatement statement = connection.prepareStatement("DELETE from characters WHERE character_health < 10;");

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("DELETEd " + count + " characters");
        getCharacters(connection);
    }


    private static void removeBossesWhereHealthEight(Connection connection) throws SQLException {

        PreparedStatement statement = connection.prepareStatement("DELETE from bosses WHERE boss_health >= 8;");

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("DELETEd " + count + " bosses");
        getBosses(connection);
    }
}

