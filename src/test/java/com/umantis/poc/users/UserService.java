package com.umantis.poc.users;

import java.util.List;

public interface UserService {

    public Integer findUserId(String userName);

    public List<String> findAllUsers();
}
