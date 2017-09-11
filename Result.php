<?php

namespace MyQEE\Database\MongoDB;

use \ArrayIterator;

/**
 * 数据库MongoDB返回对象
 *
 * @author     呼吸二氧化碳 <jonwang@myqee.com>
 * @category   Database
 * @package    Driver
 * @subpackage MongoDB
 * @copyright  Copyright (c) 2008-2017 myqee.com
 * @license    http://www.myqee.com/license.html
 */
class Result extends \MyQEE\Database\Result
{
    public function free()
    {
        $this->result = null;
    }

    public function seek($offset)
    {
        if (isset($this->data[$offset]))
        {
            return true;
        }
        elseif ($this->result instanceof ArrayIterator)
        {
            if ($this->offsetExists($offset))
            {
                $this->result->seek($offset);
                $this->currentRow = $this->internalRow = $offset;

                return true;
            }
            else
            {
                return false;
            }
        }

        if ($this->offsetExists($offset))
        {
            if ($this->internalRow < $this->currentRow)
            {
                $c = $this->internalRow - $this->currentRow;
                for($i = 0; $i < $c; $i++)
                {
                    $this->result->next();
                }
            }
            else
            {
                // 小于当前指针，则回退重新来过，因为目前 MongoCursor 还没有回退的功能
                $this->result->rewind();
                $c = $this->currentRow - $this->internalRow;

                for($i = 0; $i < $c; $i++)
                {
                    $this->result->next();
                }
            }

            $this->currentRow = $this->internalRow = $offset;

            return true;
        }
        else
        {
            return false;
        }
    }

    public function fetchAssoc()
    {
        if ($this->result instanceof ArrayIterator)
        {
            $data = $this->result->current();
            $this->result->next();

            return $data;
        }

        $data = $this->result->getNext();
        if (isset($data['_id']) && is_object($data['_id']) && $data['_id'] instanceof \MongoId)
        {
            $data['_id'] = (string)$data['_id'];
        }

        if (isset($this->query['select_as']))foreach ($this->query['select_as'] as $key => $value)
        {
            // 对查询出的数据做select as转换
            if (isset($data[$key]))
            {
                $data[$value] = $data[$key];
                unset($data[$key]);
            }
        }

        return $data;
    }

    /**
     * 使查询结果集不动态变化
     *
     * @return $this
     */
    public function snapshot()
    {
        if ($this->result && !($this->result instanceof ArrayIterator))
        {
            $this->result->snapshot();
        }

        return $this;
    }

    protected function totalCount()
    {
        if ($this->result instanceof ArrayIterator)
        {
            $count = $this->result->count();
        }
        elseif ($this->result)
        {
            $count = $this->result->count(true);
        }
        else
        {
            $count = count($this->data);
        }

        if (!$count>0)$count = 0;

        return $count;
    }
}