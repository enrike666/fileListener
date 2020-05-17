package com.company;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.CollectionType;

import java.io.FileWriter;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Main {

    public static final String LISTENED_PATH = "files\\";
    public static final String LOG_FILE_NAME = "log.log";
    public static final String REPORT_FILE_NAME = "report.txt";
    public static void main(String[] args) {


        if (args[0].equals("display")) {
            PrintReport();
        } else {
            try {
                FileWriter writer = new FileWriter(LOG_FILE_NAME, true);
                WatchService watchService = FileSystems.getDefault().newWatchService();
                Path path = Paths.get(LISTENED_PATH);
                path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

                WatchKey key;
                while ((key = watchService.take()) != null) {
                    for (WatchEvent<?> event : key.pollEvents()) {
                        String filename = event.context().toString();
                        if (!filename.endsWith("json")) {
                            writer.write(LocalDateTime.now() + " " + filename + "\n");
                            writer.flush();
                        } else {
                            Process(filename);
                        }
                    }

                    key.reset();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void Process(String filename) {
        ObjectMapper mapper = new ObjectMapper();
        try {
              String json = "";
              json = new String(Files.readAllBytes(Paths.get(LISTENED_PATH+filename)));

            CollectionType javaType = mapper.getTypeFactory()
                    .constructCollectionType(List.class, Person.class);
            List<Person> personList = mapper.readValue(json, javaType);

            System.out.println(personList.size());
            List<Person> uniquePersonList = personList.stream().distinct().collect(Collectors.toList());

            int sum = 0;
            for (Person p : uniquePersonList) {
                sum += p.getSalary();
            }
            int average = sum / uniquePersonList.size();
            List<Person> groupForDB = uniquePersonList.stream().filter(p -> p.getSalary() > average).collect(Collectors.toList());

            Connection dbConnection = DataBase.getDBConnection();
            Statement statement = dbConnection.createStatement();
            for (Person person : groupForDB) {
                String insertTableSQL = "INSERT INTO public.users " +
                        "(name, age, salary) " +
                        "VALUES ( '" + person.getName() + "', " +person.getAge() + ", " +person.getSalary()+ ");";
                statement.executeUpdate(insertTableSQL);
            }
            dbConnection.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void PrintReport()  {
        try {
            Connection dbConnection = DataBase.getDBConnection();
            Statement statement = dbConnection.createStatement();

            String selectCountUsers = "SELECT count(id) FROM public.users";
            ResultSet r = statement.executeQuery(selectCountUsers);
            while (r.next()) {
                int countRow = r.getInt("count");
                System.out.println("Общее кол-во записей в базе: " + countRow);
            }

            String selectUsersForReport = "SELECT * FROM public.users WHERE age > 25;";
            ResultSet rs = statement.executeQuery(selectUsersForReport);
            List<Person> list = new ArrayList<Person>();
            while (rs.next()) {
                list.add(new Person(
                                rs.getString("name"),
                                rs.getInt("age"),
                                rs.getInt("salary")
                        )
                );
            }

            FileWriter writer = new FileWriter(REPORT_FILE_NAME, false);
            for (Person p : list) {
                writer.write("name:"+p.getName()+", age:"+ p.getAge() + ", salary:"+ p.getSalary()+ "\n");
            }
            writer.flush();
            dbConnection.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
