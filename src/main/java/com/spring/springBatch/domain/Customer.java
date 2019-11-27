package com.spring.springBatch.domain;

import java.util.Date;

public class Customer {
    private  Integer id;
    private  Integer age;
    private Date birth;
    private Date createTime;
    private String email;
    private String lastName;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Date getBirth() {
        return birth;
    }

    public void setBirth(Date birth) {
        this.birth = birth;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id=" + id +
                ", age=" + age +
                ", birth=" + birth +
                ", createTime=" + createTime +
                ", email='" + email + '\'' +
                ", lastName='" + lastName + '\'' +
                '}';
    }
}
