# Example Cubes Info

### Dimension Cubes - Relations
![Dimension Cubes Relations](https://lucid.app/publicSegments/view/c0ae81ff-3dda-43eb-a111-5bcfeea8eab8/image.png)



### Dimension Cubes - Alternative Engines

![Dimension Cubes - Alternative Engines](https://lucid.app/publicSegments/view/c78a5b16-36b7-4d11-878d-8a7f95699d03/image.png)

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
![Fact Cubes](https://lucid.app/publicSegments/view/05e988c9-92ef-4060-8031-8f6f15692ff8/image.png)



### Update above Relationship Charts
[LucidChart Link](https://lucid.app/lucidchart/invitations/accept/c2d8586b-c7bb-4fd6-a94a-e7add2df9b41)