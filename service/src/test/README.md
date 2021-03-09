# Example Cubes Info

### Dimension Cubes - Relations
![Dimension Cubes Relations](https://user-images.githubusercontent.com/42597424/110177452-db80b880-7db9-11eb-86c8-9e266e6f5d32.jpeg)



### Dimension Cubes - Alternative Engines

![Dimension Cubes - Alternative Engines](https://user-images.githubusercontent.com/42597424/110177870-77122900-7dba-11eb-93f4-94473a88e358.jpeg)

##### dimLevel = 1 
- class
    - OracleEngine

##### dimLevel = 2 
- student
    - revision = 0
        - OracleEngine
    - revision = 1
        - OracleEngine
        - HiveEngine
        - PrestoEngine
        - DruidEngine
- researcher
    - OracleEngine
    - HiveEngine
    - PrestoEngine
    - DruidEngine
- remarks
    - DruidEngine
- class_volunteers
    - OracleEngine
- science_lab_volunteers
    - OracleEngine
- tutors
    - OracleEngine
    - HiveEngine
    - PrestoEngine
    - DruidEngine

##### dimLevel = 3
- section
    - OracleEngine
    - HiveEngine
    - PrestoEngine
    - DruidEngine
- labs
    - OracleEngine
    - HiveEngine
    - PrestoEngine
    - DruidEngine



### Fact Cubes - Alternative Engines + Dimension FK
Note: all the alternative tables in the same cubes have the same FK as its Oracle table.
![Fact Cubes](https://user-images.githubusercontent.com/42597424/110177883-7ed1cd80-7dba-11eb-8a31-b5605dc2aa11.jpeg)



### Update above Relationship Charts
[LucidChart Link](https://lucid.app/lucidchart/invitations/accept/c2d8586b-c7bb-4fd6-a94a-e7add2df9b41)