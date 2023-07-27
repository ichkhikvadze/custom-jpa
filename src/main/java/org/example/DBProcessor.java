package org.example;

import org.example.annotations.Column;
import org.example.annotations.Id;
import org.example.annotations.Table;
import org.example.dbconnection.DBConnection;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DBProcessor {

    public static boolean insert(Object object) {
        Class<?> clazz = object.getClass();

        Table table = object.getClass().getAnnotation(Table.class);

        String schemeName = table.scheme();
        String tableName = table.name();

        Field[] fields = clazz.getDeclaredFields();

        List<String> fieldNameList = getColumnFieldNameList(fields);
        List<String> annotatedFieldValueList = getAnnotatedFieldValueList(fields, Column.class, object);

        String insertQuery = buildSQLInsertQuery(fieldNameList,
                annotatedFieldValueList,
                schemeName,
                tableName);

        try (Connection conn = DBConnection.INSTANCE.getConnection();
             PreparedStatement statement = conn.prepareStatement(insertQuery)) {
            return statement.executeUpdate() == 1;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean update(Object object) {
        Table table = object.getClass().getAnnotation(Table.class);

        String schemeName = table.scheme();
        String tableName = table.name();

        Field[] fields = object.getClass().getDeclaredFields();

        List<String> columnAnnotatedFieldNameList = getColumnFieldNameList(fields);
        List<String> columnAnnotatedFieldValueList = getAnnotatedFieldValueList(fields, Column.class, object);

        List<String> idAnnotatedFieldNameList = getIdFieldNameList(fields);
        List<String> idAnnotatedFieldValueList = getAnnotatedFieldValueList(fields, Id.class, object);

        String updateQuery = buildSQLUpdateQuery(columnAnnotatedFieldNameList,
                columnAnnotatedFieldValueList,
                idAnnotatedFieldNameList,
                idAnnotatedFieldValueList,
                schemeName,
                tableName);

        try (Connection conn = DBConnection.INSTANCE.getConnection();
             PreparedStatement statement = conn.prepareStatement(updateQuery)) {
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean delete(Object object) {
        Class<?> clazz = object.getClass();

        Table table = clazz.getAnnotation(Table.class);

        String schemeName = table.scheme();
        String tableName = table.name();

        Field[] fields = clazz.getDeclaredFields();

        List<String> columnAnnotatedFieldNameList = getColumnFieldNameList(fields);
        List<String> columnAnnotatedFieldValueList = getAnnotatedFieldValueList(fields, Column.class, object);

        String deleteQuery = buildSQLDeleteQuery(columnAnnotatedFieldNameList,
                columnAnnotatedFieldValueList,
                schemeName,
                tableName
        );

        try (Connection conn = DBConnection.INSTANCE.getConnection();
             PreparedStatement statement = conn.prepareStatement(deleteQuery)) {
            return statement.executeUpdate() > 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static <E> boolean delete(Class<E> c, Object id) {
        Table table = c.getAnnotation(Table.class);

        String schemeName = table.scheme();
        String tableName = table.name();

        Optional<Field> fieldOptional = Arrays.stream(c.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .findFirst();

        if (fieldOptional.isPresent()) {
            String sqlQuery = String.format("DELETE FROM %s.%s WHERE %s = %s",
                    schemeName,
                    tableName,
                    fieldOptional.get().getAnnotation(Id.class).name(),
                    id.toString()
            );

            try (Connection conn = DBConnection.INSTANCE.getConnection();
                 PreparedStatement statement = conn.prepareStatement(sqlQuery)) {

                return statement.executeUpdate() == 1;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    public static <E> List<E> select(Class<E> c) {
        List<E> result = new ArrayList<>();

        Table table = c.getAnnotation(Table.class);

        String schemeName = table.scheme();
        String tableName = table.name();

        List<String> columnNames = getColumnNamesList(c);

        String selectQuery = String.format("SELECT * FROM %s.%s", schemeName, tableName);

        try (Connection conn = DBConnection.INSTANCE.getConnection();
             PreparedStatement statement = conn.prepareStatement(selectQuery)
        ) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                Object[] columnValues = getColumnValues(columnNames, resultSet);
                Constructor<?> constructor = Arrays.stream(c.getDeclaredConstructors())
                        .filter(cnst -> cnst.getParameterCount() == columnValues.length)
                        .findFirst().orElseThrow();
                result.add((E) constructor.newInstance(columnValues));
            }
        } catch (SQLException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static <E> E selectById(Class<E> o, Object id) {
        Table table = o.getAnnotation(Table.class);

        String schemeName = table.scheme();
        String tableName = table.name();

        Optional<Field> fieldOptional = Arrays.stream(o.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .findFirst();

        List<String> columnNames = getColumnNamesList(o);

        if (fieldOptional.isPresent()) {
            String selectByIdQuery = generateSelectByIdQuery(schemeName, tableName, fieldOptional.get(), id);
            try (Connection conn = DBConnection.INSTANCE.getConnection();
                 PreparedStatement statement = conn.prepareStatement(selectByIdQuery)
            ) {
                ResultSet resultSet = statement.executeQuery();
                if (resultSet.next()) {
                    Object[] columnValues = getColumnValues(columnNames, resultSet);
                    Constructor<?> constructor = Arrays.stream(o.getDeclaredConstructors())
                            .filter(c -> c.getParameterCount() == columnValues.length)
                            .findFirst().orElseThrow();
                    return (E) constructor.newInstance(columnValues);
                }
            } catch (SQLException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private static Object[] getColumnValues(List<String> columnNames, ResultSet resultSet) throws SQLException {
        Object[] columnValues = new Object[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            columnValues[i] = resultSet.getObject(columnNames.get(i));
        }
        return columnValues;
    }

    private static String generateSelectByIdQuery(String schemeName, String tableName, Field field, Object id) {
        return String.format(
                "SELECT * FROM %s.%s WHERE %s = %s",
                schemeName,
                tableName,
                field.getAnnotation(Column.class).name(),
                id
        );
    }

    private static <E> List<String> getColumnNamesList(Class<E> c) {
        return Arrays.stream(c.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Column.class))
                .map(f -> f.getAnnotation(Column.class).name())
                .collect(Collectors.toList());
    }

    private static String buildSQLDeleteQuery(List<String> columnAnnotatedFieldNameList,
                                              List<String> columnAnnotatedFieldValueList,
                                              String schemeName,
                                              String tableName) {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append(String.format("DELETE FROM %s.%s WHERE ", schemeName, tableName));
        appendEqualityCheck(sqlQuery, columnAnnotatedFieldNameList, columnAnnotatedFieldValueList);
        return sqlQuery.toString();
    }

    private static String buildSQLUpdateQuery(List<String> columnAnnotatedFieldNameList,
                                              List<String> columnAnnotatedFieldValueList,
                                              List<String> idAnnotatedFieldNameList,
                                              List<String> idAnnotatedFieldValueList,
                                              String schemeName,
                                              String tableName
    ) {
        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append(String.format("UPDATE %s.%s SET ", schemeName, tableName));
        appendAssignmentOperation(sqlQuery, columnAnnotatedFieldNameList, columnAnnotatedFieldValueList);
        sqlQuery.append(" WHERE ");
        appendEqualityCheck(sqlQuery, idAnnotatedFieldNameList, idAnnotatedFieldValueList);
        return sqlQuery.toString();
    }

    private static void appendEqualityCheck(StringBuilder sqlQuery,
                                            List<String> columnNameList,
                                            List<String> columnValueList) {
        for (int i = 0; i < columnNameList.size(); i++) {
            sqlQuery.append(columnNameList.get(i))
                    .append(" = ")
                    .append(columnValueList.get(i));
            if (i != columnValueList.size() - 1) {
                sqlQuery.append(" AND ");
            }
        }
    }

    private static void appendAssignmentOperation(StringBuilder sqlQuery,
                                                  List<String> columnNameList,
                                                  List<String> columnValueList) {
        for (int i = 0; i < columnNameList.size(); i++) {
            sqlQuery.append(columnNameList.get(i))
                    .append(" = ")
                    .append(columnValueList.get(i));
            if (i != columnNameList.size() - 1) {
                sqlQuery.append(", ");
            }
        }
    }

    private static String buildSQLInsertQuery(List<String> fieldNameList,
                                              List<String> annotatedFieldValueList,
                                              String schemeName,
                                              String tableName) {
        return String.format("INSERT INTO %s.%s (%s) VALUES (%s)",
                schemeName,
                tableName,
                String.join(",", fieldNameList),
                String.join(",", annotatedFieldValueList));
    }

    private static List<String> getAnnotatedFieldValueList(Field[] fields,
                                                           Class<? extends Annotation> annotationClass,
                                                           Object object) {
        return Arrays.stream(fields)
                .filter(f -> f.isAnnotationPresent(annotationClass))
                .map(f -> getValueInSQLFormat(f, object))
                .collect(Collectors.toList());
    }

    private static List<String> getIdFieldNameList(Field[] fields) {
        return Arrays.stream(fields)
                .filter(f -> f.isAnnotationPresent(Id.class))
                .map(f -> f.getAnnotation(Id.class).name())
                .collect(Collectors.toList());
    }

    private static String getValueInSQLFormat(Field f, Object object) {
        try {
            f.setAccessible(true);
            if (isNumericType(f.getType().toString())) {
                return f.get(object).toString();
            }
            return "'" + f.get(object).toString() + "'";
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return "";
    }

    private static boolean isNumericType(String fieldType) {
        return fieldType.equals("int") ||
                fieldType.equals("long") ||
                fieldType.equals("class java.lang.Integer") ||
                fieldType.equals("class java.lang.Long");
    }

    private static List<String> getColumnFieldNameList(Field[] fields) {
        return Arrays.stream(fields)
                .filter(f -> f.isAnnotationPresent(Column.class))
                .map(f -> f.getAnnotation(Column.class).name())
                .collect(Collectors.toList());
    }
}
