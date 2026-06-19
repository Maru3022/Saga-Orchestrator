package org.example.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "saga.topics")
public class SagaTopicsProperties {

    private String userCreated;
    private String notificationSend;
    private String notificationResponse;
    private String cabinetCreate;
    private String cabinetResponse;
    private String nutritionCalculate;
    private String nutritionResponse;
    private String notificationCommand;
    private String nutritionCommand;
    private String notificationResponseHyphen;
    private String nutritionResponseHyphen;
    private String trainsResponse;
    private String userResponse;
    private String trainsCommand;
    private String userCommand;
    private String notificationCompensate;
    private String cabinetCompensate;
    private String userCompensate;
    private String notificationSendDlt;
    private String cabinetCreateDlt;
    private String nutritionCalculateDlt;
    private String userCreatedDlt;
    private String notificationResponseDlt;
    private String cabinetResponseDlt;
    private String nutritionResponseDlt;

    public String getUserCreated() {
        return userCreated;
    }

    public void setUserCreated(String userCreated) {
        this.userCreated = userCreated;
    }

    public String getNotificationSend() {
        return notificationSend;
    }

    public void setNotificationSend(String notificationSend) {
        this.notificationSend = notificationSend;
    }

    public String getNotificationResponse() {
        return notificationResponse;
    }

    public void setNotificationResponse(String notificationResponse) {
        this.notificationResponse = notificationResponse;
    }

    public String getCabinetCreate() {
        return cabinetCreate;
    }

    public void setCabinetCreate(String cabinetCreate) {
        this.cabinetCreate = cabinetCreate;
    }

    public String getCabinetResponse() {
        return cabinetResponse;
    }

    public void setCabinetResponse(String cabinetResponse) {
        this.cabinetResponse = cabinetResponse;
    }

    public String getNutritionCalculate() {
        return nutritionCalculate;
    }

    public void setNutritionCalculate(String nutritionCalculate) {
        this.nutritionCalculate = nutritionCalculate;
    }

    public String getNutritionResponse() {
        return nutritionResponse;
    }

    public void setNutritionResponse(String nutritionResponse) {
        this.nutritionResponse = nutritionResponse;
    }

    public String getNotificationCommand() {
        return notificationCommand;
    }

    public void setNotificationCommand(String notificationCommand) {
        this.notificationCommand = notificationCommand;
    }

    public String getNutritionCommand() {
        return nutritionCommand;
    }

    public void setNutritionCommand(String nutritionCommand) {
        this.nutritionCommand = nutritionCommand;
    }

    public String getNotificationResponseHyphen() {
        return notificationResponseHyphen;
    }

    public void setNotificationResponseHyphen(String notificationResponseHyphen) {
        this.notificationResponseHyphen = notificationResponseHyphen;
    }

    public String getNutritionResponseHyphen() {
        return nutritionResponseHyphen;
    }

    public void setNutritionResponseHyphen(String nutritionResponseHyphen) {
        this.nutritionResponseHyphen = nutritionResponseHyphen;
    }

    public String getTrainsResponse() {
        return trainsResponse;
    }

    public void setTrainsResponse(String trainsResponse) {
        this.trainsResponse = trainsResponse;
    }

    public String getUserResponse() {
        return userResponse;
    }

    public void setUserResponse(String userResponse) {
        this.userResponse = userResponse;
    }

    public String getTrainsCommand() {
        return trainsCommand;
    }

    public void setTrainsCommand(String trainsCommand) {
        this.trainsCommand = trainsCommand;
    }

    public String getUserCommand() {
        return userCommand;
    }

    public void setUserCommand(String userCommand) {
        this.userCommand = userCommand;
    }

    public String getNotificationCompensate() {
        return notificationCompensate;
    }

    public void setNotificationCompensate(String notificationCompensate) {
        this.notificationCompensate = notificationCompensate;
    }

    public String getCabinetCompensate() {
        return cabinetCompensate;
    }

    public void setCabinetCompensate(String cabinetCompensate) {
        this.cabinetCompensate = cabinetCompensate;
    }

    public String getUserCompensate() {
        return userCompensate;
    }

    public void setUserCompensate(String userCompensate) {
        this.userCompensate = userCompensate;
    }

    public String getNotificationSendDlt() {
        return notificationSendDlt;
    }

    public void setNotificationSendDlt(String notificationSendDlt) {
        this.notificationSendDlt = notificationSendDlt;
    }

    public String getCabinetCreateDlt() {
        return cabinetCreateDlt;
    }

    public void setCabinetCreateDlt(String cabinetCreateDlt) {
        this.cabinetCreateDlt = cabinetCreateDlt;
    }

    public String getNutritionCalculateDlt() {
        return nutritionCalculateDlt;
    }

    public void setNutritionCalculateDlt(String nutritionCalculateDlt) {
        this.nutritionCalculateDlt = nutritionCalculateDlt;
    }

    public String getUserCreatedDlt() {
        return userCreatedDlt;
    }

    public void setUserCreatedDlt(String userCreatedDlt) {
        this.userCreatedDlt = userCreatedDlt;
    }

    public String getNotificationResponseDlt() {
        return notificationResponseDlt;
    }

    public void setNotificationResponseDlt(String notificationResponseDlt) {
        this.notificationResponseDlt = notificationResponseDlt;
    }

    public String getCabinetResponseDlt() {
        return cabinetResponseDlt;
    }

    public void setCabinetResponseDlt(String cabinetResponseDlt) {
        this.cabinetResponseDlt = cabinetResponseDlt;
    }

    public String getNutritionResponseDlt() {
        return nutritionResponseDlt;
    }

    public void setNutritionResponseDlt(String nutritionResponseDlt) {
        this.nutritionResponseDlt = nutritionResponseDlt;
    }
}
