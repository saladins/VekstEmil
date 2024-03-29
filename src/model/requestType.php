<?php
abstract class RequestType {
    const Detailed = 10;
    const Variable = 20;
    const Description = 30;
    const Related = 40;
    const Links = 50;
    const Tags = 60;
    const Menu = 70;
    const Search = 80;
    const Update = 100;
    const DataTables = 110;
    const VariableSettings = 120;
    const VariableUpdate = 130;
    const LinkInsert = 140;
    const LinkDelete = 150;
    const Unknown = -1;
}