project-panthera-skin
=====================
# An Analytical SQL Engine (ASE) for Hadoop #

#### Project contact: [Zhihui Li] (mailto:zhihui.li@intel.com), [Liye Zhang](mailto:liye.zhang@intel.com), [Daoyuan Wang](mailto:daoyuan.wang@intel.com)

---
### OVERVIEW ###

This project is an independent version of ["Project Panthera"](<https://github.com/intel-hadoop/project-panthera-ase>).

---
### Getting Started ###

- git clone https://github.com/adrian-wang/project-panthera-skin.git
- cd project-panthera-skin/
...........
- panthera > select a from x where b > (select max(c) from y);

---
### Feature List ###
 - 1. Support all Hive query syntax which is compatible SQL92.
 - 2. Base on 1, Panthera ASE support:

<table>
   <tr>
      <td>Feature</td>
      <td>Comment</td>
      <td>Example </td>
   </tr>
   <tr>
      <td>Multi-Table in FROM clause</td>
      <td></td>
      <td>select * from x,y where x.a=y.b </td>
   </tr>
   <tr>
      <td>Subquery in WHERE clause</td>
      <td>Not support non-equal joint condition </td>
      <td>select a from x where a = (select max(c) from y) </td>
   </tr>
   <tr>
      <td>Subquery in HAVING clause</td>
      <td>Not support non-equal joint condition </td>
      <td>select max(a) from x group by b having max(a) = (select max(c) from y) </td>
   </tr>
   <tr>
      <td>Order by column position</td>
      <td></td>
      <td>select a,b from x order by 1 </td>
   </tr>
   <tr>
      <td>Top level UNION ALL</td>
      <td></td>
      <td>select a from x union all select a from y </td>
   </tr>
</table>

 - You can also click [here](http://intel-hadoop.github.io/project-panthera-ase/) to see what ASE supports in detail.
