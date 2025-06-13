-- Complex Aggregate Function Test by Haru (´• ω •`)
-- Version 3: English strings, no DISTINCT, full table JOIN
-- Covers GROUP BY, HAVING, JOIN, and various edge cases.

-- 一、准备环境：创建表并插入多样化数据
-- 创建一个更复杂的学生表和成绩表
CREATE TABLE students (
    student_id INT,
    student_name CHAR(50),
    major CHAR(20),
    entry_year INT
);

CREATE TABLE grades (
    enrollment_id INT,
    student_id INT,
    course_name CHAR(30),
    score FLOAT
);

-- 插入学生数据，包含不同专业和入学年份
INSERT INTO students VALUES(101, 'Alice', 'CS', 2022);
INSERT INTO students VALUES(102, 'Bob', 'CS', 2022);
INSERT INTO students VALUES(103, 'Charlie', 'Physics', 2023);
INSERT INTO students VALUES(104, 'Diana', 'Physics', 2023);
INSERT INTO students VALUES(105, 'Eve', 'CS', 2023);
INSERT INTO students VALUES(106, 'Frank', 'Chemistry', 2022); -- This student has no grades
INSERT INTO students VALUES(107, 'Grace', 'CS', 2022);


-- 插入成绩数据
-- 计算机专业学生的成绩
INSERT INTO grades VALUES(1, 101, 'Database', 94.0);
INSERT INTO grades VALUES(2, 101, 'OS', 88.0);
INSERT INTO grades VALUES(3, 102, 'Database', 74.5);
INSERT INTO grades VALUES(4, 102, 'DataStructure', 75.0); -- <<<< Was NULL
INSERT INTO grades VALUES(5, 105, 'Database', 87.0);
INSERT INTO grades VALUES(6, 107, 'Database', 94.0); -- <<<< Test for duplicate scores
INSERT INTO grades VALUES(7, 107, 'DataStructure', 98.0);

-- Grades for Physics students
INSERT INTO grades VALUES(8, 103, 'Mechanics', 91.0);
INSERT INTO grades VALUES(9, 103, 'Electromagnetism', 85.5);
INSERT INTO grades VALUES(10, 104, 'Mechanics', 65.0); -- <<<< Was NULL
INSERT INTO grades VALUES(11, 104, 'Electromagnetism', 91.0);
INSERT INTO grades VALUES(12, 104, 'Thermodynamics', 70.0);

-- 二、基础聚合函数测试 (无 GROUP BY)
-- COUNT 函数测试
SELECT COUNT(*) AS total_enrollments FROM grades; -- 应该为 12
SELECT COUNT(score) AS graded_enrollments FROM grades; -- 应该为 12 (无NULL)
-- DISTINCT is not supported.
-- SELECT COUNT(DISTINCT course_name) AS distinct_courses FROM grades;

-- SUM / AVG 函数测试
SELECT SUM(score) AS total_score FROM grades; -- 94+88+74.5+75+87+94+98+91+85.5+65+91+70 = 1013
SELECT AVG(score) AS average_score FROM grades; -- 1013 / 12 = 84.416...

-- MAX / MIN 函数测试
SELECT MAX(score) AS max_score, MIN(score) AS min_score FROM grades; -- MAX: 98.0, MIN: 65.0
SELECT MAX(student_id) AS max_student_id FROM students; -- MAX: 107

-- 三、带 WHERE 条件的聚合函数测试
-- 筛选特定课程
SELECT AVG(score) AS db_avg_score FROM grades WHERE course_name = 'Database'; -- (94+74.5+87+94)/4 = 87.375
-- Test aggregation on an empty set by filtering for a non-existent course
SELECT COUNT(*) as non_existent_count, SUM(score) as non_existent_sum, AVG(score) as non_existent_avg FROM grades WHERE course_name = 'non_existent_course'; -- Should return 0, NULL, NULL

-- 筛选分数范围
SELECT COUNT(*) FROM grades WHERE score > 90; -- 应该为 5 (94, 98, 91, 91, 94)

-- 四、GROUP BY 测试
-- 按课程分组统计
SELECT course_name, COUNT(*) AS num_students, AVG(score) AS avg_score, MAX(score) AS max_score
FROM grades
GROUP BY course_name;

-- 按学生ID分组
SELECT student_id, COUNT(course_name) as num_courses, SUM(score) as total_score
FROM grades
GROUP BY student_id;

-- 五、GROUP BY 与 HAVING 结合测试
-- 找出平均分高于 90 分的课程
SELECT course_name, AVG(score)
FROM grades
GROUP BY course_name
HAVING AVG(score) > 90; -- 'DataStructure' and 'Electromagnetism'

-- 找出选课数超过1门的同学
SELECT student_id, COUNT(*) as num_courses
FROM grades
GROUP BY student_id
HAVING COUNT(*) > 1; -- 101, 102, 103, 104, 107

-- 六、JOIN 与聚合函数结合测试 (Full Table Join)
-- 按专业统计学生的平均分
SELECT s.major, AVG(g.score) as major_avg_score
FROM students s, grades g
WHERE s.student_id = g.student_id
GROUP BY s.major;

-- 统计2022年入学的学生中，每个人的平均分
SELECT s.student_name, AVG(g.score)
FROM students s, grades g
WHERE s.student_id = g.student_id AND s.entry_year = 2022
GROUP BY s.student_id, s.student_name; -- student_id也要group, 因为name可能重名

-- 七、更多边界情况
-- 创建一个空表进行测试
CREATE TABLE empty_table (id INT, val FLOAT);
SELECT COUNT(*), SUM(val), AVG(val), MAX(val), MIN(val) FROM empty_table; -- 应该返回 0, NULL, NULL, NULL, NULL
DROP TABLE empty_table;

-- 八、清理环境
DROP TABLE grades;
DROP TABLE students;