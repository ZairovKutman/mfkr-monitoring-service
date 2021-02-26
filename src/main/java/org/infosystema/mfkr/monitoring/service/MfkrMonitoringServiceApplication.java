package org.infosystema.mfkr.monitoring.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.scheduling.annotation.Scheduled;

import java.sql.Types;
import java.util.Calendar;

@SpringBootApplication
public class MfkrMonitoringServiceApplication {

    private final JdbcTemplate jdbcTemplate;
    private final Integer year = Calendar.getInstance().get(Calendar.YEAR);

    @Autowired
    public MfkrMonitoringServiceApplication(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public static void main(String[] args) {
        SpringApplication.run(MfkrMonitoringServiceApplication.class, args);
    }

    //everyday at 1:00
    @Scheduled(cron = "0 0 16 * * *", zone = "Asia/Almaty")
    public void updateIncomeFact() {

        SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate)
                .withFunctionName("get_data_income_fact")
                .declareParameters(new SqlParameter("arg_year", Types.INTEGER), new SqlParameter("on_date", Types.DATE));

        Integer out = simpleJdbcCall.executeFunction(Integer.class, new MapSqlParameterSource("arg_year", year));
        if (out == 1) {
            updateSpendingsFact();
            System.out.println("Finish update updateIncomeFact function");
        } else {
            System.out.println("didn't finish update updateIncomeFact function");
        }
    }

    //everyday at 3:00
//    @Scheduled(cron = "0 0 3 * * *", zone = "Asia/Almaty")
    public void updateSpendingsFact() {
        SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate)
                .withFunctionName("get_data_spendings_fact")
                .declareParameters(new SqlParameter("arg_year", Types.INTEGER), new SqlParameter("on_date", Types.DATE));

        Integer out = simpleJdbcCall.executeFunction(Integer.class, new MapSqlParameterSource("arg_year", year));
        if (out == 1) {
            updateSpendingsPlan();
            System.out.println("Finish update updateSpendingsFact function");
        } else {
            System.out.println("didn't finish update updateSpendingsFact function");
        }
    }

    //everyday at 5:00
//    @Scheduled(cron = "0 0 5 * * *", zone = "Asia/Almaty")
    public void updateSpendingsPlan() {

        SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate)
                .withFunctionName("get_data_spendings_plan")
                .declareParameters(new SqlParameter("arg_year", Types.INTEGER), new SqlParameter("on_date", Types.DATE));

        Integer out = simpleJdbcCall.executeFunction(Integer.class, new MapSqlParameterSource("arg_year", year));
        if (out == 1) {
            System.out.println("Finish update updateSpendingsPlan function");
        } else {
            System.out.println("didn't finish update updateSpendingsPlan function");
        }
    }
}