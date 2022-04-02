package com.example.sparkdemo.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import javax.persistence.*;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(indexes = {@Index(columnList = "name"), @Index(columnList = "keyinfo") ,@Index(columnList = "description"),@Index(columnList = "id")},

        uniqueConstraints = {
                @UniqueConstraint(columnNames={ "description", "keyinfo", "name"})
        }

)
public class DeviceStatisticsData implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    Long id;

    String keyinfo;

    String name;

    String value;

    String description;

    String extend;

}
