/* Модуль БД */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;


/**
 *Класс, предназначенный для создания и управления базой данных чата.
 */
public class ChatDB {

    //Служебные поля для работы с БД. Применяются почти всеми методами.
    private Connection connection = null;
    private Statement statement = null;
    private String sql = null;

    //Имена таблиц и столбцов. Использовать " вместо '.
    private final String USER_TABLE = "users";
    private final String USER_TABLE_NAME = "name";
    private final String USER_TABLE_PASSWORD = "password";
    private final String USER_TABLE_NICKNAME = "nickname";

    private final String MESSAGE_TABLE = "messages";
    private final String MESSAGE_TABLE_NICKNAME = "nickname";
    private final String MESSAGE_TABLE_MESSAGE_TEXT = "messageText";
    private final String MESSAGE_TABLE_DATE = "date";
    private final String MESSAGE_TABLE_IP = "ip";


    /**
     *Конструктор по умолчанию подключается к существующей базе данных. Если база данных не обнаружена, то она будет
     * создана автоматически.
     */
    public ChatDB() {
        //Загружаем драйвера БД.
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            System.err.println("Ошибка при загрузке драйверов БД");
            e.printStackTrace();
            System.exit(1);
        }

        //Создаём БД, затем подключаемся к ней. Только подключаемся, если БД уже есть.
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:chat.db");
        } catch (SQLException e) {
            System.err.println("Ошибка при создании БД или при подключении к ней");
            e.printStackTrace();
            System.exit(1);
        }

        //Создаём таблицу для хранения пользователей. Если она уже есть - ничего не делаем.
        try {
            statement = connection.createStatement();
            sql = String.format("CREATE TABLE IF NOT EXISTS %s" +
                                "(" +
                                "%s TEXT," +
                                "%s TEXT," +
                                "%s TEXT," +
                                "PRIMARY KEY (%s, %s)" +
                                ");",
                                    USER_TABLE,
                                    USER_TABLE_NAME,
                                    USER_TABLE_PASSWORD,
                                    USER_TABLE_NICKNAME,
                                    USER_TABLE_NAME,
                                    USER_TABLE_NICKNAME);
            statement.executeUpdate(sql);
            closeConnectionWithCatch();

        } catch (SQLException e) {
            System.err.println("Ошибка при создании таблицы пользователей");
            e.printStackTrace();
            closeConnectionWithCatch();
            System.exit(1);
        }

        //Создаём таблицу для хранения сообщений. Если она уже есть - ничего не делаем.
        try {
            statement = connection.createStatement();
            sql = String.format("CREATE TABLE IF NOT EXISTS %s" +
                                "(" +
                                "%s TEXT," +
                                "%s TEXT," +
                                "%s TEXT," +
                                "%s TEXT" +
                                ")",
                                    MESSAGE_TABLE,
                                    MESSAGE_TABLE_NICKNAME,
                                    MESSAGE_TABLE_MESSAGE_TEXT,
                                    MESSAGE_TABLE_DATE,
                                    MESSAGE_TABLE_IP);
            statement.executeUpdate(sql);
            closeConnectionWithCatch();

        } catch (SQLException e) {
            System.err.println("Ошибка при создании таблицы сообщений");
            e.printStackTrace();
            closeConnectionWithCatch();
            System.exit(1);
        }
    }


    /**
     * Добавляет нового пользователя с именем <code>name</code>, паролем <code>password</code> и никнеймом
     * <code>nickname</code> в базу данных.
     * @param name имя пользователя
     * @param password пароль пользователя
     * @param nickname никнейм пользователя
     * @return <p><code>true</code>, если пользователь добавлен успешно</p>
     * <p><code>false</code>, если не удалось добавить пользователя</p>
     */
    public synchronized boolean addUser(String name, String password, String nickname) {
        try {
            statement = connection.createStatement();
            sql = String.format("INSERT INTO %s (%s, %s, %s) VALUES ('%s', '%s', '%s');",
                                USER_TABLE,
                                USER_TABLE_NAME,
                                USER_TABLE_PASSWORD,
                                USER_TABLE_NICKNAME,
                                    name.replace("'", "''"),
                                    password.replace("'", "''"),
                                    nickname.replace("'", "''")
            );
            statement.executeUpdate(sql);

            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return true;
            }

            return true;

        } catch (SQLException e1) {
            e1.printStackTrace();
            closeConnectionWithCatch();
            return false;
        }
    }


    /**
     * Добавляет новое сообщение с никнеймом отправителя <code>nickname</code>, текстом сообщения
     * <code>messageText</code>, датой отправки <code>date</code>, и ip отправителя <code>ip</code> в базу данных.
     * @param nickname никнейм пользователя
     * @param messageText текст сообщения
     * @param date дата отправки сообщения
     * @param ip ip отправителя
     * @return <p><code>true</code>, если сообщение добавлено успешно</p>
     * <p><code>false</code>, если не удалось добавить сообщение</p>
     */
    public synchronized boolean addMessage(String nickname,
                                           String messageText,
                                           String date,
                                           String ip) {
        try {
            statement = connection.createStatement();
            sql = String.format("INSERT INTO %s (%s, %s, %s, %s) VALUES ('%s', '%s', '%s', '%s');",
                                    MESSAGE_TABLE,
                                    MESSAGE_TABLE_NICKNAME,
                                    MESSAGE_TABLE_MESSAGE_TEXT,
                                    MESSAGE_TABLE_DATE,
                                    MESSAGE_TABLE_IP,
                                        nickname.replace("'", "''"),
                                        messageText.replace("'", "''"),
                                        date.replace("'", "''"),
                                        ip.replace("'", "''")
            );
            statement.executeUpdate(sql);

            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return true;
            }

            return true;

        } catch (SQLException e1) {
            e1.printStackTrace();
            closeConnectionWithCatch();
            return false;
        }
    }


    /**
     * Проверяет, есть ли пользователь с именем <code>name</code> в базе данных. Если есть, то совпадает ли его
     * пароль с паролем <code>password</code>.
     * @param name имя пользователя
     * @param password пароль пользователя
     * @return <p><code>true</code>, если пользователь с данным именем существует и пароли совпадают.</p>
     * <p><code>false</code>, если пользователь с данным именем не существует, либо есть пользователь с
     * данным именем, но пароли не совпадают.</p>
     */
    public synchronized boolean checkUserPassword(String name, String password) {
        try {
            statement = connection.createStatement();
            sql = String.format("SELECT * FROM %s WHERE %s='%s' AND %s='%s';",
                                    USER_TABLE,
                                    USER_TABLE_NAME,
                                    name.replace("'", "''"),
                                    USER_TABLE_PASSWORD,
                                    password.replace("'", "''")
            );
            ResultSet rawRecords = statement.executeQuery(sql);

            if (rawRecords.next() == true) {
                if (rawRecords.getString(USER_TABLE_PASSWORD).equals(password)) {
                    closeConnectionWithCatch();
                    return true;
                } else {
                    closeConnectionWithCatch();
                    return false;
                }
            } else {
                closeConnectionWithCatch();
                return false;
            }

        } catch (SQLException e1) {
            e1.printStackTrace();
            closeConnectionWithCatch();
            return false;
        }
    }


    /**
     * Возвращает последние <code>X</code> сообщений из базы данных.
     * Каждое сообщение представляет из себя список строковых значений (<code>ArrayList&lt;String&gt;</code>), где
     * первый элемент - это никнейм отправителя, второй элемент - текст его сообщения, третий элемент - статус
     * отправителя, четвёртый элемент - дата сообщения, пятый элемент - IP отправителя. Все найденные сообщения, в свою
     * очередь, помещаются в единый список (<code>ArrayList&lt;ArratList&lt;String&gt;&gt;</code>), который и
     * возвращается из данного метода.
     * @param X количество требуемых сообщений
     * @return <p>Список сообщений вида <code>ArrayList&lt;ArratList&lt;String&gt;&gt;</code>, если найдено хотя бы
     * одно сообщение</p>
     * <p><code>null</code>, если при извлечении из БД произошла ошибка</p>
     */
    public synchronized ArrayList<ArrayList<String>> getLastXMessages(int X) {

        ArrayList<ArrayList<String>> messages = new ArrayList<>();

        try {
            statement = connection.createStatement();
            sql = String.format("SELECT * FROM %s ORDER BY rowid DESC LIMIT %s",
                    MESSAGE_TABLE,
                    X
            );
            ResultSet rawRecords = statement.executeQuery(sql);

            while (rawRecords.next()) {
                ArrayList<String> message = new ArrayList<>();
                message.add(rawRecords.getString(MESSAGE_TABLE_NICKNAME));
                message.add(rawRecords.getString(MESSAGE_TABLE_MESSAGE_TEXT));
                message.add(rawRecords.getString(MESSAGE_TABLE_DATE));
                message.add(rawRecords.getString(MESSAGE_TABLE_IP));
                messages.add(message);
            }

            ArrayList<ArrayList<String>> messagesAscending = new ArrayList<>();
            for (int i = messages.size() - 1; i >= 0; i--) {
                messagesAscending.add(messages.get(i));
            }
            messages = messagesAscending;


            closeConnectionWithCatch();
            return messages;

        } catch (SQLException e1) {
            e1.printStackTrace();
            closeConnectionWithCatch();
            return null;
        }
    }


    /**
     * Проверяет, есть ли пользователь с именем <code>name</code> в базе данных.
     * @param name искомое имя пользователя
     * @return <p><code>true</code>, если пользователь с данным именем найден в базе данных</p>
     * <p><code>false</code>, если пользователь с данным именем не найден в базе данных</p>
     */
    public synchronized boolean isNameInDB(String name) {
        try {
            statement = connection.createStatement();
            sql = String.format("SELECT * FROM %s WHERE %s='%s';",
                                    USER_TABLE,
                                    USER_TABLE_NAME,
                                    name.replace("'", "''")
            );
            ResultSet rawRecords = statement.executeQuery(sql);

            if (rawRecords.next() == true) {
                if (rawRecords.getString(USER_TABLE_NAME).equals(name)) {
                    closeConnectionWithCatch();
                    return true;
                } else {
                    closeConnectionWithCatch();
                    return false;
                }
            } else {
                closeConnectionWithCatch();
                return false;
            }

        } catch (SQLException e1) {
            e1.printStackTrace();
            closeConnectionWithCatch();
            return false;
        }
    }


    /**
     * Проверяет, есть ли пользователь с данным никнеймом в базе данных.
     * @param nickname искомый никнейм пользователя
     * @return <p><code>true</code>, если пользователь с данным никнеймом найден</p>
     * <p><code>false</code>, если пользователь с данным никнеймом не найден</p>
     */
    public synchronized boolean isNicknameInDB(String nickname) {
        try {
            statement = connection.createStatement();
            sql = String.format("SELECT * FROM %s WHERE %s='%s';",
                                    USER_TABLE,
                                    USER_TABLE_NICKNAME,
                                    nickname.replace("'", "''")
            );
            ResultSet rawRecords = statement.executeQuery(sql);

            if (rawRecords.next() == true) {
                if (rawRecords.getString(USER_TABLE_NICKNAME).equals(nickname)) {
                    closeConnectionWithCatch();
                    return true;
                } else {
                    closeConnectionWithCatch();
                    return false;
                }
            } else {
                closeConnectionWithCatch();
                return false;
            }

        } catch (SQLException e1) {
            e1.printStackTrace();
            closeConnectionWithCatch();
            return false;
        }
    }


    /**
     * Извлекает никнейм пользователя с данным именем <code>name</code>
     * @param name имя пользователя, чей никнейм необходимо получить
     * @return <p>Никнейм пользователя, если был найден пользователей с именем <code>name</code></p>
     * <p><code>null</code>, если никнейм для данного имени <code>name</code> не был найден, либо произошла ошибка
     * при извлечении из БД</p>
     */
    public synchronized String getNickname(String name) {

        String nickname = null;

        try {
            statement = connection.createStatement();
            sql = String.format("SELECT * FROM %s WHERE %s = '%s';",
                                    USER_TABLE,
                                    USER_TABLE_NAME,
                                    name.replace("'", "''")
            );
            ResultSet rawRecords = statement.executeQuery(sql);

            while (rawRecords.next()) {
                nickname = rawRecords.getString(USER_TABLE_NICKNAME);
            }

            if (nickname == null) {
                closeConnectionWithCatch();
                return null;
            } else {
                closeConnectionWithCatch();
                return nickname;
            }

        } catch (SQLException e1) {
            e1.printStackTrace();
            closeConnectionWithCatch();
            return null;
        }
    }


    private void closeConnectionWithCatch() {
        try {
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}