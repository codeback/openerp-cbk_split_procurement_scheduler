# -*- coding: utf-8 -*-
##############################################################################
#
#    OpenERP, Open Source Management Solution
#    Copyright (C) 2004-2010 Tiny SPRL (<http://tiny.be>).
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################

from datetime import datetime
from dateutil.relativedelta import relativedelta
from openerp import netsvc
from openerp import pooler
from openerp.osv import osv
from openerp.osv import fields
from openerp.tools.translate import _
from openerp.tools import DEFAULT_SERVER_DATE_FORMAT, DEFAULT_SERVER_DATETIME_FORMAT
from openerp import tools

from random import randint

import logging

class procurement_order(osv.osv):
    _inherit = 'procurement.order'

    def run_scheduler_split(self, cr, uid, automatic=False, use_new_cursor=False, exceptions=True, subset=150, context=None):
        logging.info("\nSplit_MRP_Sched_Start: %s - %s - %s - %d\n" % (automatic, use_new_cursor, exceptions, subset))

        ''' Runs through scheduler.
        @param use_new_cursor: False or the dbname
        '''
        if use_new_cursor:
            use_new_cursor = cr.dbname

        # Se modifica '_procure_confirm' para ejecutar menos abastecimientos y que de tiempo a 
        # completar la ejecución

        self._procure_confirm_split(cr, uid, use_new_cursor=use_new_cursor, exceptions=exceptions, subset=subset, context=context)    
        
        logging.info("\nSplit_MRP_Sched_Middle\n")
        self._procure_orderpoint_confirm(cr, uid, automatic=automatic,\
           use_new_cursor=use_new_cursor, context=context)    

        logging.info("\nSplit_MRP_Sched_Finish\n")

    def _procure_confirm_split(self, cr, uid, ids=None, use_new_cursor=False, exceptions=True, subset=150, context=None):
        '''
        Call the scheduler to check the procurement order

        @param self: The object pointer
        @param cr: The current row, from the database cursor,
        @param uid: The current user ID for security checks
        @param ids: List of selected IDs
        @param use_new_cursor: False or the dbname
        @param context: A standard dictionary for contextual values
        @return:  Dictionary of values
        '''
        if context is None:
            context = {}
        try:
            if use_new_cursor:
                cr = pooler.get_db(use_new_cursor).cursor()
            wf_service = netsvc.LocalService("workflow")

            procurement_obj = self.pool.get('procurement.order')

            if exceptions:
                if not ids:
                    ids = procurement_obj.search(cr, uid, [('state', '=', 'exception')])               

                qty_procs = len(ids)
                    
                for i, id in enumerate(ids, 1):
                    wf_service.trg_validate(uid, 'procurement.order', id, 'button_restart', cr)
                    
                    if ((i%500 == 0) or (qty_procs == i)) and use_new_cursor:
                        cr.commit()
                
            company = self.pool.get('res.users').browse(cr, uid, uid, context=context).company_id
            maxdate = (datetime.today() + relativedelta(days=company.schedule_range)).strftime(tools.DEFAULT_SERVER_DATE_FORMAT)
            start_date = fields.datetime.now()
            offset = 0
            report = []
            report_total = 0
            report_except = 0
            report_later = 0

            logging.info("\nSplit_MRP_Sched_Step (1): before make_to_order")
            counter = 0
            while True:
                ids = procurement_obj.search(cr, uid, [('state', '=', 'confirmed'), ('procure_method', '=', 'make_to_order')], offset=offset, limit=500, order='priority, date_planned', context=context)
                logging.info("\nSplit_MRP_Sched_Step (1A): make_to_order. length: %d. subset: %d." % (len(ids), subset))
                
                ids_subset = ids[0:subset]
                
                for proc in procurement_obj.browse(cr, uid, ids_subset, context=context):
                    counter = counter + 1
                    logging.info("\nSplit_MRP_Sched_Step (1B): make_to_order n%d:  %s - %s - %s" % (counter, proc.name, proc.origin, proc.state))
                    if maxdate >= proc.date_planned:
                        wf_service.trg_validate(uid, 'procurement.order', proc.id, 'button_check', cr)
                    else:
                        offset += 1
                        report_later += 1

                    if proc.state == 'exception':
                        report.append(_('PROC %d: on order - %3.2f %-5s - %s') % \
                                (proc.id, proc.product_qty, proc.product_uom.name,
                                    proc.product_id.name))
                        report_except += 1
                    report_total += 1
                if use_new_cursor:
                    cr.commit()
                if not ids:
                    break
            offset = 0
            ids = []            

            logging.info("\nSplit_MRP_Sched_Step (2): before make_to_stock")
            counter = 0
            while True:
                report_ids = []
                ids = procurement_obj.search(cr, uid, [('state', '=', 'confirmed'), ('procure_method', '=', 'make_to_stock')], offset=offset)

                logging.info("\nSplit_MRP_Sched_Step (2A): make_to_stock. length: %d. subset: %d." % (len(ids), subset))

                ids_subset = ids[0:subset]

                for proc in procurement_obj.browse(cr, uid, ids_subset):
                    counter = counter + 1
                    logging.info("\nSplit_MRP_Sched_Step (2B): make_to_stock n%d:  %s - %s - %s" % (counter, proc.name, proc.origin, proc.state))
                    if maxdate >= proc.date_planned:
                        wf_service.trg_validate(uid, 'procurement.order', proc.id, 'button_check', cr)
                        report_ids.append(proc.id)
                    else:
                        report_later += 1
                    report_total += 1

                    if proc.state == 'exception':
                        report.append(_('PROC %d: from stock - %3.2f %-5s - %s') % \
                                (proc.id, proc.product_qty, proc.product_uom.name,
                                    proc.product_id.name,))
                        report_except += 1

                if use_new_cursor:
                    cr.commit()
                offset += len(ids)
                if not ids: break
            end_date = fields.datetime.now()

            if use_new_cursor:
                cr.commit()
        finally:            
            if use_new_cursor:
                try:
                    cr.close()
                except Exception:
                    pass
        return {}

    

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4:
